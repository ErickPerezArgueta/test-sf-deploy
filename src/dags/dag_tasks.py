import configparser
import os
import sys
print(sys.path)

from typing import *

from snowflake.snowpark.session import Session
from snowflake.snowpark.version import VERSION

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task import StoredProcedureCall
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation

# Ensure the parent directory of `src` is in sys.path

from imports_train_pipeline.process_func import process_data
from imports_train_pipeline.train_func import train_register


my_dir = os.path.dirname(os.path.realpath(__file__))

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

try:
    session.sql(f"""REMOVE @{dict_creds['database']}.{dict_creds['schema']}.ML_MODELS/TRAIN_PIPELINE/""").collect()
except Exception as e:
    print(f"Error removing TRAIN_PIPELINE: {e}")

try:
    session.sql(f"""REMOVE @{dict_creds['database']}.{dict_creds['schema']}.ML_MODELS/INFERENCE_PIPELINE/""").collect()
except Exception as e:
    print(f"Error removing INFERENCE_PIPELINE: {e}")


with DAG("DAG_TRAIN") as dag_train:
    dag_task1_train = DAGTask(
        "process",
        StoredProcedureCall(
            func=process_data,
            stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.ML_MODELS/TRAIN_PIPELINE/PROCESS",
            packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
            imports=['src/dags/imports_train_pipeline']
        ),
        warehouse="COMPUTE_WH"
    )
    dag_task2_train = DAGTask(
        "train_register",
        StoredProcedureCall(
            func=train_register,
            stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.ML_MODELS/TRAIN_PIPELINE/TRAIN",
            packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
            imports=['src/dags/imports_train_pipeline']
        ),
        warehouse="COMPUTE_WH"
    )

dag_task1_train >> dag_task2_train


# with DAG("DAG_INFERENCE") as dag_inference:
#     dag_task1_inference = DAGTask(
#         "process",
#         StoredProcedureCall(
#             func=process_data,
#             stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.ML_MODELS/INFERENCE_PIPELINE/PROCESS",
#             packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
#             imports=[os.path.join(my_dir, 'process_func.py')]
#         ),
#         warehouse="COMPUTE_WH"
#     )
#     dag_task2_inference = DAGTask(
#         "train_register",
#         StoredProcedureCall(
#             func=train_register,
#             stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.ML_MODELS/INFERENCE_PIPELINE/INFERENCE",
#             packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
#             imports=[os.path.join(my_dir, 'train_func.py')]
#         ),
#         warehouse="COMPUTE_WH"
#     )

# dag_task1_inference >> dag_task2_inference



root_train = Root(session)
schema_train = root_train.databases[dict_creds['database']].schemas[dict_creds['schema']]
dag_op_train = DAGOperation(schema_train)
dag_op_train.deploy(dag_train, CreateMode.or_replace)
dag_op_train.run(dag_train)


# root_inference = Root(session)
# schema_inference = root_inference.databases[dict_creds['database']].schemas[dict_creds['schema']]
# dag_op_inference = DAGOperation(schema_inference)
# dag_op_inference.deploy(dag_inference, CreateMode.or_replace)
