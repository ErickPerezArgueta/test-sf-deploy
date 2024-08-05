import configparser
import os
import sys
import subprocess
import yaml


from typing import *

from snowflake.snowpark.session import Session
from snowflake.snowpark.version import VERSION

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task import StoredProcedureCall
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation

from imports_train_pipeline.process_func import process_data
from imports_train_pipeline.train_func import train_register
from imports_inference_pipeline.process_inference_func import process_data_inference
from imports_inference_pipeline.inference_func import train_register_inference


# def load_local_variables(current_environment):
#     with open("config.yaml", "r") as file:
#         config = yaml.safe_load(file)

#     if current_environment not in config['environments']:
#         raise ValueError(f"Environment '{current_environment}' not found in configuration file")

#     current_env_config = config['environments'][current_environment]
#     available_environments = config['environments']
#     ml_application_environments = [env for env in available_environments if env != current_environment]

#     if current_environment not in ml_application_environments:
#         return current_env_config
#     else:
#         sys.exit(1)


def load_config(env_name):
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    env_config = config['environments'].get(env_name)

    if not env_config:
        raise ValueError(f"Environment '{env_name}' not found in config.yml")

    if env_name in ['validate', 'live']:
        # Definir las variables específicas que esperamos encontrar en el entorno
        expected_vars = [
            'ACCOUNTNAME', 'USERNAME', 'PASSWORD', 'ROLENAME',
            'DBNAME', 'WAREHOUSENAME', 'SF_SCHEMA', 'STAGE_NAME',
            'TRAIN_DIR', 'INFERENCE_DIR', 'MODEL_NAME'
        ]
        
        # Reemplazar valores en el diccionario con variables de entorno
        env_config = {var.lower(): os.getenv(var) for var in expected_vars}

        # Validar que todas las variables esperadas están presentes en el entorno
        missing_vars = [var for var in expected_vars if env_config[var.lower()] is None]
        if missing_vars:
            raise EnvironmentError(f"Missing environment variables: {', '.join(missing_vars)}")
    
    return env_config


def create_snowpark_session(env_var):
    connections_parameters = {
        'account': env_var['accountname'],
        'user': env_var['username'],
        'password': env_var['password'],
        'role': env_var['rolename'],
        'database': env_var['dbname'],
        'warehouse': env_var['warehousename'],
        'schema': env_var['sf_schema']
    }

    session = Session.builder.configs(connections_parameters).create()

    return session

# def create_remote_session(env_var):
#     my_dir = os.path.dirname(os.path.realpath(__file__))

#     config = configparser.ConfigParser()
#     config_path = os.path.expanduser("~/.snowsql/config") 
#     config.read(config_path)
#     stage_name=os.getenv("STAGE_NAME")
#     train_dir=os.getenv("TRAIN_DIR")
#     inference_dir=os.getenv("INFERENCE_DIR")
#     model_name=os.getenv("MODEL_NAME")

#     dict_creds = {}

#     #Se comenta esta linea de codigo para usar el json con credenciales dentro del proyecto
#     dict_creds['account'] = config[f'connections']['accountname']
#     dict_creds['user'] = config[f'connections']['username']
#     dict_creds['password'] = config[f'connections']['password']
#     dict_creds['role'] = config[f'connections']['rolename']
#     dict_creds['dbname'] = config[f'connections']['dbname']
#     dict_creds['warehouse'] = config[f'connections']['warehousename']
#     dict_creds['schemaname'] = config[f'connections']['schemanamename']

#     session = Session.builder.configs(dict_creds).create()



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <environment>")
        sys.exit(1)

    current_environment = sys.argv[1]
    env_var = load_config(current_environment)
    
    session = create_snowpark_session(env_var)


    try:
        session.sql(f"""REMOVE @{env_var['dbname']}.{env_var['sf_schema']}.{env_var['stage_name']}/{env_var['train_dir']}/""").collect()
        session.sql(f"""REMOVE @{env_var['dbname']}.{env_var['sf_schema']}.{env_var['stage_name']}/{env_var['inference_dir']}/""").collect()
    except:
        print(["Prueba de except"])


    with DAG(f"{env_var['model_name']}_TRAIN") as dag_train:
        dag_task1_train = DAGTask(
            "process",
            StoredProcedureCall(
                func=process_data,
                stage_location=f"@{env_var['dbname']}.{env_var['sf_schema']}.{env_var['stage_name']}/{env_var['train_dir']}/PROCESS",
                packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
                imports=['src/dags/imports_train_pipeline']
            ),
            warehouse="COMPUTE_WH"
        )
        dag_task2_train = DAGTask(
            "train_register",
            StoredProcedureCall(
                func=train_register,
                stage_location=f"@{env_var['dbname']}.{env_var['sf_schema']}.{env_var['stage_name']}/{env_var['train_dir']}/TRAIN",
                packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
                imports=['src/dags/imports_train_pipeline']
            ),
            warehouse="COMPUTE_WH"
        )

    dag_task1_train >> dag_task2_train


    with DAG(f"{env_var['model_name']}_INFERENCE") as dag_inference:
        dag_task1_inference = DAGTask(
            "process",
            StoredProcedureCall(
                func=process_data_inference,
                stage_location=f"@{env_var['dbname']}.{env_var['sf_schema']}.{env_var['stage_name']}/{env_var['inference_dir']}/PROCESS",
                packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
                imports=['src/dags/imports_inference_pipeline']
            ),
            warehouse="COMPUTE_WH"
        )
        dag_task2_inference = DAGTask(
            "train_register",
            StoredProcedureCall(
                func=train_register_inference,
                stage_location=f"@{env_var['dbname']}.{env_var['sf_schema']}.{env_var['stage_name']}/{env_var['inference_dir']}/INFERENCE",
                packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
                imports=['src/dags/imports_inference_pipeline']
            ),
            warehouse="COMPUTE_WH"
        )

    dag_task1_inference >> dag_task2_inference



    root_train = Root(session)
    schemaname_train = root_train.databases[env_var['dbname']].schemas[env_var['sf_schema']]
    dag_op_train = DAGOperation(schemaname_train)
    dag_op_train.deploy(dag_train, CreateMode.or_replace)
    dag_op_train.run(dag_train)


    root_inference = Root(session)
    schemaname_inference = root_inference.databases[env_var['dbname']].schemas[env_var['sf_schema']]
    dag_op_inference = DAGOperation(schemaname_inference)
    dag_op_inference.deploy(dag_inference, CreateMode.or_replace)



