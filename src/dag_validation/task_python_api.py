import json
from snowflake.snowpark import Session
from snowflake.core.task import TaskCollection
from snowflake.core import Root

def create_snowpark_session(config_path):
    # Read the JSON configuration file
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Extract the configuration details
    connection_parameters = {
        "account": config["account"],
        "user": config["user"],
        "password": config["password"],
        "role": config["role"]
    }

    # Create and return the Snowpark session
    session = Session.builder.configs(connection_parameters).create()
    return session

# Usage
config_path = r'C:\Users\erick.a.perez\OneDrive - Accenture\Documents\erick.a.perez\PersonalProjects\test-sf-deploy\creds.json'
session = create_snowpark_session(config_path)
root = Root(session)


tasks: TaskCollection = root.databases['banana_quality'].schemas['exp'].tasks
task_iter = tasks.iter()  # returns a PagedIter[Task]
# print(task_iter)
# print(type(task_iter))
# for task_obj in task_iter:
#   print(task_obj.name)

MODEL_NAME = 'GATO' 

# Variable para verificar si el modelo existe
model_exists = False

# Iterar a través del iterador de tareas y verificar si algún nombre de tarea contiene MODEL_NAME
for task_obj in task_iter:
    if MODEL_NAME in task_obj.name:
        model_exists = True
        break  # No es necesario seguir iterando si encontramos una coincidencia

if model_exists:
    print(f"Un modelo con el nombre '{MODEL_NAME}' ya existe.")
else:
    print(f"No existe ningún modelo con el nombre '{MODEL_NAME}'.")
