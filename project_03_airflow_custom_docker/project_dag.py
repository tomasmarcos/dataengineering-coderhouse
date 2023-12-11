# Imports
# Dag utils ---
from dotenv import load_dotenv

load_dotenv()
import os
from datetime import datetime, timedelta
from typing import Dict, List
import sys

# DAG
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Custom script imports
sys.path.insert(0, "/root/airflow/dags/scripts")
from scripts.etl_utils import (
    create_table,
    prepare_insert_values,
    query_to_df,
    retrieve_tables,
    filter_tables,
    insert_values,
)

# Credentials & parameter definitions
api_token = os.getenv("API_TOKEN")
redshift_credentials = {
    "host": os.getenv("REDSHIFT_HOST"),
    "dbname": os.getenv("REDSHIFT_DBNAME"),
    "user": os.getenv("REDSHIFT_USER"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
    "port": os.getenv("REDSHIFT_PORT"),
}
schema_name = "m_tomasmarcos_coderhouse"


# apis to query
api_urls_to_query = dict(
    milestones="https://api.estadisticasbcra.com/milestones",
    blue_usd="https://api.estadisticasbcra.com/usd",
    official_usd="https://api.estadisticasbcra.com/usd_of",
)


start_date = datetime.now() - timedelta(days=1)
start_date = start_date.strftime("%Y-%m-%d")
end_date = datetime.now().strftime("%Y-%m-%d")


# ETL Main function
def run_pipeline(start_date, end_date) -> None:
    """
    Executes an ETL pipeline consisting of retrieving, filtering, and inserting tables.

    Returns:
    - None: The function does not return anything.

    Note:
    - This function assumes the existence of global variables: , 'redshift_credentials',
      'api_urls_to_query', 'api_token'.
    - The pipeline involves three steps:
      1. Retrieve tables from API URLs.
      2. Filter tables to include only data from a specified date.
      3. Insert filtered table values into a Redshift database.

    Example:
    The function can be called to perform an ETL pipeline with the specified global variables and dependencies.
    """

    print(f"[INFO] ETL START date: {start_date}; END date: {end_date}")

    # Step 1: Retrieve tables (all because this API doest not allow to get soem specific rows )
    print(
        "[INFO] Starting step1 retrieve_tables (all because this API doest not allow to get soem specific rows) "
    )
    retrieved_tables = retrieve_tables(api_urls_to_query, api_token)
    # Step2: Filter out tables to add data from a certain day
    print("[INFO] Starting step2 - Filter data only for a the selected date.")
    filtered_tables = filter_tables(
        retrieved_tables, start_date=start_date, end_date=end_date
    )
    # These are the only tables to append since are the only ones that have values to insert
    print(f"[INFO] Tables with more than one value to append: {filtered_tables.keys()}")
    # Step 3: Insert the values
    print("[INFO] Starting step3 - Insert values")
    insert_values(filtered_tables, redshift_credentials, schema_name)
    print("[INFO] Insert values Completed successfully")


default_args = {
    "owner": "Tom",
    "retries": 0
    # "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="centralbank_api_statistics_etl",
    default_args=default_args,
    description="Single Node DAG ETL Pipeline retrieving statistical information from an unnoficial api that contains argentinian economic indicators",
    start_date=datetime(2023, 12, 7, 2),
    schedule_interval="@daily",
) as dag:
    single_task = PythonOperator(
        task_id="pipeline_allsteps",
        python_callable=run_pipeline,
        op_kwargs={"start_date": start_date, "end_date": end_date},
        execution_timeout=timedelta(seconds=300)
    )
