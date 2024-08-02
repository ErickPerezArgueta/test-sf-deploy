import configparser
import os
import sys
import subprocess
import yaml


from typing import *

# from snowflake.snowpark.session import Session
# from snowflake.snowpark.version import VERSION

# from snowflake.core import Root
# from snowflake.core._common import CreateMode
# from snowflake.core.task import StoredProcedureCall
# from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation

# from imports_train_pipeline.process_func import process_data
# from imports_train_pipeline.train_func import train_register
# from imports_inference_pipeline.process_inference_func import process_data_inference
# from imports_inference_pipeline.inference_func import train_register_inference


# def set_environment():
#     if len(sys.argv) != 2:
#         print("Usage: python main.py <environment>")
#         sys.exit(1)

#     environment = sys.argv[1]

#     # Execute the setup script with the environment argument
#     result = subprocess.run(["python", "setup.py", environment], capture_output=True, text=True)

#     print(type(result))

#     if result.returncode != 0:
#         print("Failed to setup environment")
#         print(result.stderr)
#         sys.exit(result.returncode)

#     print(result.stdout)


def load_config(current_environment):
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    if current_environment in config['environments']:
        return config['environments'][current_environment], config['environments']
    else:
        raise ValueError(f"Environment '{current_environment}' not found in configuration file")


def load_variables(current_env, current_env_config, available_environments):
    ml_application_environments = [env for env in available_environments if env != current_env]
    
    print(f'Las aplicaciones de ML son: {ml_application_environments}')

    if current_env not in ml_application_environments:
        globals().update(current_env_config)
        print("Variables loaded into globals.")
    else:
        print("Environment is either 'pre' or 'prod'. No variables loaded.")



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <environment>")
        sys.exit(1)

    current_environment = sys.argv[1]
 
   # Load configuration
    current_env_config, available_environments = load_config(current_environment)

    load_variables(
        current_env=current_environment, 
        current_env_config=current_env_config, 
        available_environments=available_environments
        )
    
    model_name=globals().get('MODEL_NAME')
    print(model_name)
    

    # print(var_env)
    
   

    # Set variables as globals
    # globals().update(config)
    # my_dir = os.path.dirname(os.path.realpath(__file__))
    # stage_name=globals().get('STAGE_NAME')
    # train_dir=globals().get('TRAIN_DIR')
    # inference_dir=globals().get('INFERENCE_DIR')
    # environment=globals().get('ENV_NAME')
    
    #print(stage_name)

    # # Access the values using the section and key
    # # Assuming the values you want are in the "connections.dev" section
    # dict_creds = {}

    # #Se comenta esta linea de codigo para usar el json con credenciales dentro del proyecto
    # dict_creds['account'] = config[f'connections.{environment}']['accountname']
    # dict_creds['user'] = config[f'connections.{environment}']['username']
    # dict_creds['password'] = config[f'connections.{environment}']['password']
    # dict_creds['role'] = config[f'connections.{environment}']['rolename']
    # dict_creds['database'] = config[f'connections.{environment}']['dbname']
    # dict_creds['warehouse'] = config[f'connections.{environment}']['warehousename']
    # dict_creds['schema'] = config[f'connections.{environment}']['schemaname']

    # session = Session.builder.configs(dict_creds).create()
    # session.use_database(dict_creds['database'])
    # session.use_schema(dict_creds['schema'])


    # try:
    #     session.sql(f"""REMOVE @{dict_creds['database']}.{dict_creds['schema']}.{stage_name}/{train_dir}/""").collect()
    #     session.sql(f"""REMOVE @{dict_creds['database']}.{dict_creds['schema']}.{stage_name}/{inference_dir}/""").collect()
    # except:
    #     print(["Prueba de except"])


    # with DAG(f"{model_name}_TRAIN") as dag_train:
    #     dag_task1_train = DAGTask(
    #         "process",
    #         StoredProcedureCall(
    #             func=process_data,
    #             stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.{stage_name}/{train_dir}/PROCESS",
    #             packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
    #             imports=['src/dags/imports_train_pipeline']
    #         ),
    #         warehouse="COMPUTE_WH"
    #     )
    #     dag_task2_train = DAGTask(
    #         "train_register",
    #         StoredProcedureCall(
    #             func=train_register,
    #             stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.{stage_name}/{train_dir}/TRAIN",
    #             packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
    #             imports=['src/dags/imports_train_pipeline']
    #         ),
    #         warehouse="COMPUTE_WH"
    #     )

    # dag_task1_train >> dag_task2_train


    # with DAG(f"{model_name}_INFERENCE") as dag_inference:
    #     dag_task1_inference = DAGTask(
    #         "process",
    #         StoredProcedureCall(
    #             func=process_data_inference,
    #             stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.{stage_name}/{inference_dir}/PROCESS",
    #             packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
    #             imports=['src/dags/imports_inference_pipeline']
    #         ),
    #         warehouse="COMPUTE_WH"
    #     )
    #     dag_task2_inference = DAGTask(
    #         "train_register",
    #         StoredProcedureCall(
    #             func=train_register_inference,
    #             stage_location=f"@{dict_creds['database']}.{dict_creds['schema']}.{stage_name}/{inference_dir}/INFERENCE",
    #             packages=['snowflake-ml-python', 'snowflake-snowpark-python'],
    #             imports=['src/dags/imports_inference_pipeline']
    #         ),
    #         warehouse="COMPUTE_WH"
    #     )

    # dag_task1_inference >> dag_task2_inference



    # root_train = Root(session)
    # schema_train = root_train.databases[dict_creds['database']].schemas[dict_creds['schema']]
    # dag_op_train = DAGOperation(schema_train)
    # dag_op_train.deploy(dag_train, CreateMode.or_replace)
    # dag_op_train.run(dag_train)


    # root_inference = Root(session)
    # schema_inference = root_inference.databases[dict_creds['database']].schemas[dict_creds['schema']]
    # dag_op_inference = DAGOperation(schema_inference)
    # dag_op_inference.deploy(dag_inference, CreateMode.or_replace)



