from snowflake.snowpark.session import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.ml.modeling.xgboost import XGBClassifier
from snowflake.ml.modeling.metrics import roc_auc_score
from snowflake.ml.modeling.metrics import roc_curve
from snowflake.ml.registry import Registry
import io
import re

from typing import *
import pandas as pd
import warnings
from typing  import *
warnings.filterwarnings("ignore")
import configparser
import os
import joblib

config = configparser.ConfigParser()
config_path = os.path.expanduser("~/.snowsql/config") 
config.read(config_path)

# Access the values using the section and key
# Assuming the values you want are in the "connections.dev" section
dict_creds = {}

#Se comenta esta linea de codigo para usar el json con credenciales dentro del proyecto
dict_creds['account'] = config['connections.dev']['accountname']
dict_creds['user'] = config['connections.dev']['username']
dict_creds['password'] = config['connections.dev']['password']
dict_creds['role'] = config['connections.dev']['rolename']
dict_creds['database'] = config['connections.dev']['dbname']
dict_creds['warehouse'] = config['connections.dev']['warehousename']
dict_creds['schema'] = config['connections.dev']['schemaname']

session = Session.builder.configs(dict_creds).create()
session.use_database(dict_creds['database'])
session.use_schema(dict_creds['schema'])


#Functions
def train_model(session: Session, db_name: str, schema_name: str, train_table: str) -> XGBClassifier:
    #session.use_database(db_name)
    #session.use_schema(schema_name)
    df_train_banana = session.table(train_table)
    feature_cols = df_train_banana.columns
    feature_cols.remove('QUALITY')
    target_col = 'QUALITY'
    xgbmodel = XGBClassifier(random_state=123, input_cols=feature_cols, label_cols=target_col, output_cols='PREDICTION')
    xgbmodel.fit(df_train_banana)
    return xgbmodel

# if __name__ == "__main__":

#     xgbmodel = train_model(session, "BANANA_QUALITY", "DEV", "BANANA_TRAIN")
#     xgb_file = xgbmodel.to_xgboost()
#     MODEL_FILE = 'model.joblib.gz'
#     joblib.dump(xgb_file, MODEL_FILE)
#     session.file.put(MODEL_FILE, "@DEV.ML_MODELS", auto_compress=False, overwrite=True)


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
    # Filtrar el DataFrame por el nombre del modelo
    #model_df = df[df['name'] == model_name]

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
 

def main(sess: Session) -> T.Variant:

    xgbmodel = train_model(sess,  dict_creds['database'],  dict_creds['schema'], "BANANA_TRAIN")
    xgb_file = xgbmodel.to_xgboost()
    MODEL_FILE = 'model.joblib.gz'
    # joblib.dump(xgb_file, MODEL_FILE)
    # sess.file.put(MODEL_FILE, "@DEV.ML_MODELS", auto_compress=False, overwrite=True)
    buffer = io.BytesIO()
    joblib.dump(xgb_file, buffer)
    buffer.seek(0)

    sess.file.put_stream(buffer, f"@{dict_creds['schema']}.ML_MODELS/{MODEL_FILE}", auto_compress=False, overwrite=True)

    metrics_train = get_metrics(sess,dict_creds['database'], dict_creds['schema'], "BANANA_TRAIN", xgbmodel)
    metrics_test = get_metrics(sess,dict_creds['database'], dict_creds['schema'], "BANANA_TEST", xgbmodel)

    mv = register_model(session=sess, db_name=dict_creds['database'], schema_name=dict_creds['schema'],model=xgbmodel, model_name="BANANA_MODEL", metrics_train=metrics_train, metrics_test=metrics_test)

    return str(f'El entrenamiento y registro en model registry se realizó de manera exitosa')


sproc = session.sproc.register(func=main,
                                  name='train_step',
                                  is_permanent=True,
                                  replace=True,
                                  stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.ML_MODELS",
                                  execute_as='caller',
                                  packages=['snowflake-ml-python==1.2.3',
                                            'snowflake-snowpark-python==1.13.0'
                                           ])


    


 
 

    


