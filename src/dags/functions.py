import io
import re
import joblib
import pandas as pd

from typing import *

from snowflake.snowpark.session import Session
from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

from snowflake.ml.modeling.xgboost import XGBClassifier
from snowflake.ml.modeling.metrics import roc_auc_score
from snowflake.ml.modeling.metrics import roc_curve
from snowflake.ml.registry import Registry

## Funciones process ##
def read_table_sf(session: Session, db_name: str, schema_name: str, table_name: str) -> DataFrame: 
    df = session.table(f'{db_name}.{schema_name}.{table_name}')
    return df

def transform_to_numeric_target(df: DataFrame) -> DataFrame:
    df_proc = df.withColumn(
        "QUALITY", F.when(df["QUALITY"] == "Good", 1).otherwise(0)
    )
    return df_proc

def write_df_to_sf(df: DataFrame, db_name: str, schema_name: str, table_name: str) -> None:
    df.write.mode("overwrite").save_as_table(f'{db_name}.{schema_name}.{table_name}')

def split_proc(session: Session, db_name: str, schema_name: str, table_name: str)-> None:
    df_proc = read_table_sf(session, db_name, schema_name, table_name)
    df_train, df_test = df_proc.random_split([0.8, 0.2], seed=99)
    write_df_to_sf(df_train, db_name, schema_name, "BANANA_TRAIN")
    write_df_to_sf(df_test, db_name, schema_name, "BANANA_TEST")


def process_data(session: Session) -> T.Variant:
    db = session.get_current_database().strip('"')
    schema = session.get_current_schema().strip('"')

    df_raw = read_table_sf(session, db, schema, "BANANA_QUALITY_RAW")
   
    df_proc = transform_to_numeric_target(df_raw)
    write_df_to_sf(df_proc, db, schema, "BANANA_QUALITY_PROCESSED")

    split_proc(session, db , schema, "BANANA_QUALITY_PROCESSED")
    
    return str(f'El procesamiento se realizó de manera exitosa')

## Funciones train-register ##
def train_model(session: Session, train_table: str) -> XGBClassifier:
    df_train_banana = session.table(train_table)
    feature_cols = df_train_banana.columns
    feature_cols.remove('QUALITY')
    target_col = 'QUALITY'
    xgbmodel = XGBClassifier(random_state=123, input_cols=feature_cols, label_cols=target_col, output_cols='PREDICTION')
    xgbmodel.fit(df_train_banana)
    return xgbmodel

def get_metrics(session: Session, db_name: str, schema_name: str, table_name: str, model: XGBClassifier)-> Dict[str, str]:
    df = session.table(f"{db_name}.{schema_name}.{table_name}")
    predict_on_df = model.predict_proba(df)
    predict_clean = predict_on_df.drop('PREDICT_PROBA_0', 'PREDICTION').withColumnRenamed('PREDICT_PROBA_1', 'PREDICTION')
    roc_auc = roc_auc_score(df=predict_clean, y_true_col_names="QUALITY", y_score_col_names="PREDICTION")
    fpr, tpr, _ = roc_curve(df=predict_clean,  y_true_col_name="QUALITY", y_score_col_name="PREDICTION")
    ks = max(tpr-fpr)
    gini = 2*roc_auc-1
    metrics = {
        "roc_auc" : roc_auc,
        "ks" : ks,
        "gini" : gini
    }
    return metrics

def next_version(model_name: str, df: pd.DataFrame)-> str:
    model_df = df[df['name'] == model_name]
    versions_str = model_df['versions'].iloc[0]
    version_list_str = re.findall(r'\d+', versions_str)
    version_numbers = [int(version) for version in version_list_str]  # Convertir 'V1' a 1, 'V2' a 2, etc.
    
    # Encontrar la versión más alta y devolver la siguiente versión
    next_version_number = max(version_numbers) + 1
    next_version_string = 'V' + str(next_version_number)
    
    return next_version_string

def register_model(
    session: Session,
    db_name: str, 
    schema_name: str, 
    model: XGBClassifier,
    model_name: str, 
    metrics_train: Dict[str, str],
    metrics_test: Dict[str, str]
    ) -> Registry:

    reg = Registry(session=session, database_name=db_name,  schema_name=schema_name)
    model_info = reg.show_models()

    if model_info.empty:
        mv = reg.log_model(
        model=model, 
        model_name=f"{model_name}",
        version_name="V0",
        metrics={
            "roc_auc_train":metrics_train["roc_auc"],
            "ks_train":metrics_train["ks"],
            "gini_train":metrics_train["gini"],
            "roc_auc_test":metrics_test["roc_auc"],
            "ks_test":metrics_test["ks"],
            "gini_test":metrics_test["gini"]
        })
    else:
        updated_version = next_version(model_name, model_info)
        mv = reg.log_model(
        model=model, 
        model_name=f"{model_name}",
        version_name=updated_version,
        metrics={
            "roc_auc_train":metrics_train["roc_auc"],
            "ks_train":metrics_train["ks"],
            "gini_train":metrics_train["gini"],
            "roc_auc_test":metrics_test["roc_auc"],
            "ks_test":metrics_test["ks"],
            "gini_test":metrics_test["gini"]
        })
    
    return mv

def train_register(session: Session) -> T.Variant:
    db = session.get_current_database().strip('"')
    schema = session.get_current_schema().strip('"')

    xgbmodel = train_model(session, "BANANA_TRAIN")
    xgb_file = xgbmodel.to_xgboost()
    MODEL_FILE = 'model.joblib.gz'
    buffer = io.BytesIO()
    joblib.dump(xgb_file, buffer)
    buffer.seek(0)

    session.file.put_stream(buffer, f"@{schema}.ML_MODELS/{MODEL_FILE}", auto_compress=False, overwrite=True)

    metrics_train = get_metrics(session, db, schema, "BANANA_TRAIN", xgbmodel)
    metrics_test = get_metrics(session, db, schema, "BANANA_TEST", xgbmodel)

    mv = register_model(session=session, db_name=db, schema_name=schema, model=xgbmodel, model_name="BANANA_MODEL", metrics_train=metrics_train, metrics_test=metrics_test)

    return str(f'El entrenamiento y registro en model registry se realizó de manera exitosa')