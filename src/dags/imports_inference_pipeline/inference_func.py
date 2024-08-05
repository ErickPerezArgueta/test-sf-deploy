import re
import pandas as pd

from typing import *

from snowflake.snowpark.session import Session
import snowflake.snowpark.types as T

from snowflake.ml.modeling.xgboost import XGBClassifier
from snowflake.ml.modeling.metrics import roc_auc_score
from snowflake.ml.modeling.metrics import roc_curve
from snowflake.ml.registry import Registry


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

def train_register_inference(session: Session) -> T.Variant:
    db = session.get_current_database().strip('"')
    schema = session.get_current_schema().strip('"')

    xgbmodel = train_model(session, "BANANA_TRAIN")
    
    metrics_train = get_metrics(session, db, schema, "BANANA_TRAIN", xgbmodel)
    metrics_test = get_metrics(session, db, schema, "BANANA_TEST", xgbmodel)

    register_model(session=session, db_name=db, schema_name=schema, model=xgbmodel, model_name="BANANA_MODEL", metrics_train=metrics_train, metrics_test=metrics_test)

    return str(f'El entrenamiento y registro en model registry se realizó de manera exitosa')