import configparser
import os

from datetime import timedelta
from typing import *

from snowflake.snowpark.session import Session
from snowflake.snowpark.version import VERSION

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task import StoredProcedureCall
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation

from functions import process_data, train_register

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

with DAG("MY_DAG", schedule=timedelta(minutes=2)) as dag:
    dag_task1 = DAGTask(
        "process",
        StoredProcedureCall(
            func=process_data,
            stage_location="@ML_MODELS",
            packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
            imports=[os.path.join(my_dir, 'functions.py')]
        ),
        warehouse="COMPUTE_WH"
    )
    dag_task2 = DAGTask(
        "train_register",
        StoredProcedureCall(
            func=train_register,
            stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.ML_MODELS",
            packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
            imports=[os.path.join(my_dir, 'functions.py')]
        ),
        warehouse="COMPUTE_WH"
    )

dag_task1 >> dag_task2

root = Root(session)
schema = root.databases[dict_creds['database']].schemas[dict_creds['schema']]
dag_op = DAGOperation(schema)
dag_op.deploy(dag, CreateMode.or_replace)