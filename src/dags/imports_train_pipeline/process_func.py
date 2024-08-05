
from typing import *

from snowflake.snowpark.session import Session
from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T


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

