from snowflake.snowpark.session import Session
from snowflake.snowpark import DataFrame
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from typing import *
import warnings
from typing  import *
warnings.filterwarnings("ignore")
import configparser
import os

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
# TO DO: CONFIGURACIÓN PARA DEFINIR VERSIONES DE PAQUETES A UTILIZAR.
#session.custom_package_usage_config = {"enabled": True}


#Functions
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
    # session.use_database(f'{db_name}')
    # session.use_schema(f'{schema_name}')

    df_proc = read_table_sf(session, db_name, dict_creds['schema'], table_name)
    df_train, df_test = df_proc.random_split([0.8, 0.2], seed=99)
    write_df_to_sf(df_train, db_name, schema_name, "BANANA_TRAIN")
    write_df_to_sf(df_test, db_name, schema_name, "BANANA_TEST")


def main(sess: Session) -> T.Variant:

    # sess.use_database("BANANA_QUALITY")
    # sess.use_schema("FEATURE_ENGINEERING")
    df_raw = read_table_sf(sess, dict_creds['database'], dict_creds['schema'], "BANANA_QUALITY_RAW")
   
    df_proc = transform_to_numeric_target(df_raw)
    write_df_to_sf(df_proc, dict_creds['database'],dict_creds['schema'], "BANANA_QUALITY_PROCESSED")

    split_proc(sess, dict_creds['database'], dict_creds['schema'], "BANANA_QUALITY_PROCESSED")
    #sess.close()
    
    return str(f'El procesamiento se realizó de manera exitosa')


sproc = session.sproc.register(func=main,
                                  name='process_step',
                                  is_permanent=True,
                                  replace=True,
                                  stage_location=f"@{dict_creds['database']}.{ dict_creds['schema']}.ML_MODELS",
                                  packages=[
                                            'snowflake-ml-python==1.2.3',
                                            'snowflake-snowpark-python==1.13.0'
                                           ])

#TO DO
#Propuesta de versiones de paquets
#snowflke-ml-python==1.5.1',
#snowflake-snowpark-python==1.17.0


    


 
 

    


