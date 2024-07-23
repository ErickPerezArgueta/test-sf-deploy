import os
from snowflake import connector
import argparse

def main():

    #Configurar el parser de argumentos
    parser = argparse.ArgumentParser(description='Ejecutar script SQL en Snowflake.')
    parser.add_argument('--sql_file', type=str, required=True, help='Ruta del archivo SQL a ejecutar')
    args = parser.parse_args()
    print(f"proviene de argumento: {args.sql_file}")

    # Obtener los valores de los secrets de GitHub
    account = os.environ.get('SNOWSQL_ACCOUNT')
    user = os.environ.get('SNOWSQL_USER')
    password = os.environ.get('SNOWSQL_PWD')
    db_name = os.environ.get('SNOWSQL_DATABASE')
    schema_name = os.environ.get('SNOWFLAKE_SCHEMA')
    role = os.environ.get('SNOWSQL_ROLE')
    wh = os.environ.get('SNOWSQL_WAREHOUSE')

    parent_dir = os.path.dirname(os.environ['GITHUB_WORKSPACE'])
    print(f"directorio padre {parent_dir}")

    print(f"sin directorio padre: {os.environ['GITHUB_WORKSPACE']}")


    file_path = os.path.join(os.environ['GITHUB_WORKSPACE'], args.sql_file) #args.sql_file
    print(f"filepath completo: {file_path}")

    # Leer el script SQL
    with open(file_path, 'r') as file:
        sql_script = file.read()

    #Diccionario de variables a sustituir
    replacements = {
        '$DB_NAME$':db_name,
        '$SCHEMA$':schema_name,
        '$ROLE$':role,
        '$WAREHOUSE$':wh
    }

    # Realizar los reemplazos solo si la variable existe
    for variable, value in replacements.items():
        if value:
            sql_script = sql_script.replace(variable, value)

    # Separar las declaraciones SQL por punto y coma
    sql_statements = sql_script.split(';')

    conn = connector.connect(
        user=user,
        password=password,
        account=account,
        database=db_name,
        schema=schema_name
    )

    try:
        with conn.cursor() as cursor:
            for statement in sql_statements:
                if statement.strip():  # Ignorar líneas en blanco
                    print(f"Ejecutando: {statement.strip()}")
                    cursor.execute(statement)
    except Exception as e:
        print(f"Error ejecutando SQL:{e}")

    conn.close()


if __name__ == "__main__":
    main()