import os

## SCRIPT ---
import os
import requests
from tqdm import tqdm
import os
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

# Credentials
api_token = os.getenv("API_TOKEN")
redshift_credentials = {
    "host": os.getenv("REDSHIFT_HOST"),
    "dbname": os.getenv("REDSHIFT_DBNAME"),
    "user": os.getenv("REDSHIFT_USER"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
    "port": os.getenv("REDSHIFT_PORT"),
}
schema_name = "m_tomasmarcos_coderhouse"


# Step 0 : Define some functions


def create_table(table_name: str, schema_name: str, dataframe: pd.DataFrame) -> str:
    """
    Generates SQL schema for creating a table in the specified schema based on the given dataframe's structure.

    Parameters:
    - table_name (str): Name of the SQL table to be created.
    - schema_name (str): Name of the SQL schema where the table will be created.
    - dataframe (pd.DataFrame): DataFrame whose structure will be used to define the table schema.

    Returns:
    - str: SQL query string to create the table within the specified schema with the appropriate columns and data types.

    Note:
    This function supports the following data types mapping:
    int64 -> INT
    int32 -> INT
    float64 -> FLOAT
    object -> VARCHAR(300)
    bool -> BOOLEAN
    datetime64[ns] -> DATE
    """
    type_map = {
        "int64": "INT",
        "int32": "INT",
        "float64": "FLOAT",
        "object": "VARCHAR(300)",
        "bool": "BOOLEAN",
        "datetime64[ns]": "DATE",
    }
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {', '.join(column_defs)}
        );
        """
    return table_schema


def prepare_insert_values(
    table_name: str, schema_name: str, dataframe: pd.DataFrame
) -> str:
    """
    Generates SQL insert statements for the given dataframe's rows into the specified table within a given schema.

    Parameters:
    - table_name (str): Name of the SQL table to insert the values.
    - schema_name (str): Name of the SQL schema where the table resides.
    - dataframe (pd.DataFrame): DataFrame whose values will be prepared for insertion.

    Returns:
    - str: SQL query string to insert the dataframe's rows into the specified table within the given schema.

    Note:
    This function converts the 'date' column of the dataframe to string before insertion.
    """
    cols = dataframe.columns.tolist()
    dataframe["date"] = dataframe["date"].astype(str)
    values_str = ",\n\t\t".join([str(tuple(x)) for x in dataframe.to_numpy()])
    insert_sql = f"""INSERT INTO {schema_name}.{table_name}
              ({', '.join( cols )})
     VALUES {values_str};
    """
    return insert_sql


def query_to_df(query):
    """
    Executes a SQL query using a predefined connection to a Redshift database and returns the result as a pandas DataFrame.

    Parameters:
    - query (str): SQL query string to be executed.

    Returns:
    - pd.DataFrame: DataFrame representation of the SQL query results.

    Note:
    This function utilizes global Redshift credentials for establishing a connection.
    Ensure that 'redshift_credentials' is properly defined and accessible before invoking this function.
    """
    conn = psycopg2.connect(**redshift_credentials)
    cur = conn.cursor()
    cur.execute(query)
    retrieved_query = cur.fetchall()
    colnames = [x.name for x in cur.description]
    df_query = pd.DataFrame(retrieved_query, columns=colnames)
    cur.close()
    conn.close()
    return df_query


# apis to query
api_urls_to_query = dict(
    milestones="https://api.estadisticasbcra.com/milestones",
    blue_usd="https://api.estadisticasbcra.com/usd",
    official_usd="https://api.estadisticasbcra.com/usd_of",
)


headers = {"Authorization": f"Bearer {api_token}"}


def retrieve_tables(api_urls_to_query):
    retrieved_tables = dict()
    # retrieve tables an convert them to pd dataframes
    for tablename, api_url in tqdm(api_urls_to_query.items()):
        print(f"[INFO] Requesting: {api_url} with token {api_token}")
        print(f"Header: {headers}")
        result = requests.get(api_url, headers=headers)
        print(f"[INFO] Printing result: {result}")
        print(f"[INFO] Printing Status: {result.status_code}")
        print(f"[INFO] Printing Content : {result.content}")
        result_json = result.json()
        df = pd.DataFrame(result_json).rename(
            columns={"d": "date", "e": "event", "v": "value", "t": "event_type"}
        )
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        retrieved_tables[tablename] = df
    return retrieved_tables


def filter_tables(retrieved_tables, filter_date):
    table_names_all = list(retrieved_tables.keys())
    filtered_tables = dict()
    for table_name in table_names_all:
        table = retrieved_tables[table_name]
        filtered_table = table[table["date"] == filter_date]
        if not filtered_table.empty:
            filtered_tables[table_name] = filtered_table
    return filtered_tables


def insert_values(filtered_tables):
    for table_name, dataframe in filtered_tables.items():
        conn = psycopg2.connect(**redshift_credentials)
        table_schema = create_table(
            table_name=table_name, schema_name=schema_name, dataframe=dataframe
        )
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute(table_schema)
        insert_sql = prepare_insert_values(
            table_name=table_name, schema_name=schema_name, dataframe=dataframe
        )
        cur.execute(insert_sql)
        cur.execute("COMMIT")
        cur.close()
        conn.close()

    print("[INFO] ETL Completed successfully")


def run_pipeline():
    print(f"[INFO] Script Current Working Directory: {os.getcwd()}")
    print(f"[INFO] Credentials lol: {redshift_credentials}")
    filter_date = datetime.now()-timedelta(days=1)
    filter_date = filter_date.strftime("%Y-%m-%d")
    print(f"[INFO] ETL Filter date: {filter_date}")

    # Step 1: Retrieve tables (all because this API doest not allow to get soem specific rows )
    print(
        "[INFO] Starting step1 Retrieve tables (all because this API doest not allow to get soem specific rows "
    )
    retrieved_tables = retrieve_tables(api_urls_to_query)

    # Step2: Filter out tables to add data from a certain day
    print("[INFO] Starting step2 - Filter data only for a the selected date.")
    filtered_tables = filter_tables(retrieved_tables, filter_date=filter_date)
    # These are the only tables to append since are the only ones that have values to insert
    print(f"[INFO] Tables with more than one value to append: {filtered_tables.keys()}")

    # Step 3: Insert the values
    print("[INFO] Starting step3 - Insert values")
    insert_values(filtered_tables)


# END SCRIPT ---


# DAG
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "Tom",
    "retries": 1,
    "retry_delay": timedelta(
        minutes=1
    ),  # 2 min de espera antes de cualquier re intento
}

with DAG(
    dag_id="project_dag_withbash_V2",
    default_args=default_args,
    description="Este es el primer DAG que creamos",
    start_date=datetime(
        2022, 8, 1, 2
    ),  # esto dice que debemos iniciar el 1-Ago-2022 y a un intervalo diario
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="primera_tarea",
        bash_command="echo hola mundo, esta es nuestra primera tarea!",
    )

    task2 = BashOperator(task_id="segunda_tarea", bash_command="pwd")

    task3 = PythonOperator(
        task_id="tercera_tarea", python_callable=run_pipeline, op_kwargs=None
    )

    task1.set_downstream(task2)
    task2.set_downstream(task3)
