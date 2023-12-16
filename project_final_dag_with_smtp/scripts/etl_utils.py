import requests
from tqdm import tqdm
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from typing import Dict, List


class StatusCodeError(Exception):
    pass


# Step 0 : Define some functions
def create_table(
    table_name: str,
    schema_name: str,
    dataframe: pd.DataFrame,
    date_primary_key=True,
    diststyle="all",
) -> str:
    """
    Generates SQL schema for creating a table in the specified schema based on the given dataframe's structure.

    Parameters:
    - table_name (str): Name of the SQL table to be created.
    - schema_name (str): Name of the SQL schema where the table will be created.
    - dataframe (pd.DataFrame): DataFrame whose structure will be used to define the table schema.
    - date_primary_key (bool): True if you want to use the date field as your Primary key, else false.
    - diststyle (str): distribution table style, see:
        https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html
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
        )
        """

    if date_primary_key:
        table_schema = table_schema.replace("DATE", "DATE NOT NULL PRIMARY KEY")
    table_schema += f"DISTSTYLE {diststyle};"
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


def query_to_df(query: str, redshift_credentials: Dict[str, str]) -> pd.DataFrame:
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


def retrieve_tables(
    api_urls_to_query: Dict[str, str], api_token: str
) -> Dict[str, pd.DataFrame]:
    """
    Retrieves tables from multiple  URLs using the provided API token and converts them to pandas DataFrames.

    Args:
    - api_urls_to_query (Dict[str, str]): A dictionary where keys are table names, and values are corresponding API URLs.
    - api_token (str): The API token used for authentication in API requests.

    Returns:
    - Dict[str, pd.DataFrame]: A dictionary where keys are table names, and values are corresponding pandas DataFrames
      containing the retrieved data.

    Raises:
    - StatusCodeError: If any of the API requests does not return a status code of 200.

    Example:
    If api_urls_to_query = {'table1': 'api_url1', 'table2': 'api_url2'} and api_token = 'your_token',
    the output would be a dictionary containing tables retrieved from the specified API URLs.
    Each DataFrame will have columns 'date', 'event', 'value', and 'event_type' after renaming.

    Note:
    - This function uses the 'requests' library for API requests.
    - The 'tqdm' library is used to display progress information.
    """
    retrieved_tables = dict()
    headers = {"Authorization": f"Bearer {api_token}"}
    # retrieve tables an convert them to pd dataframes
    for tablename, api_url in tqdm(api_urls_to_query.items()):
        result = requests.get(api_url, headers=headers)
        if result.status_code != 200:
            raise StatusCodeError(
                f"You did not get 200 as status code, status code is: {result.status_code}"
            )
        result_json = result.json()
        df = pd.DataFrame(result_json).rename(
            columns={"d": "date", "e": "event", "v": "value", "t": "event_type"}
        )
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        retrieved_tables[tablename] = df
    return retrieved_tables


def filter_tables(
    retrieved_tables: Dict[str, pd.DataFrame], start_date: str, end_date: str
) -> Dict[str, pd.DataFrame]:
    """
    Filters tables based on a date range.

    Args:
    - retrieved_tables (Dict[str, pd.DataFrame]): A dictionary where keys are table names, and values are corresponding
      pandas DataFrames.
    - start_date (str): The start date (closed) of the date range to filter the tables. It should match the 'date' column in the tables.
    - end_date (str): The end date (open) of the date range to filter the tables. It should match the 'date' column in the tables.

    Returns:
    - Dict[str, pd.DataFrame]: A dictionary where keys are table names, and values are corresponding pandas DataFrames
      containing rows with dates within the specified range.

    Example:
    If retrieved_tables = {'table1': DataFrame1, 'table2': DataFrame2},
    start_date = '2023-01-01', and end_date = '2023-01-31', and assuming 'date' is a column in all DataFrames,
    the output would be a dictionary containing only the tables that have rows with dates between '2023-01-01' and '2023-01-31'.
    """
    table_names_all = list(retrieved_tables.keys())
    filtered_tables = dict()
    for table_name in table_names_all:
        table = retrieved_tables[table_name]
        filtered_table = table[
            (table["date"] >= start_date) & (table["date"] < end_date)
        ]
        if not filtered_table.empty:
            filtered_tables[table_name] = filtered_table
    return filtered_tables


def insert_values(
    filtered_tables: Dict[str, pd.DataFrame],
    redshift_credentials: Dict[str, str],
    schema_name: str,
) -> None:
    """
    Inserts values from filtered tables into corresponding tables in a Redshift database.

    Args:
    - filtered_tables (Dict[str, pd.DataFrame]): A dictionary where keys are table names, and values are corresponding
      pandas DataFrames containing filtered data to be inserted.
    - redshift_credentials (Dict[str, str]): A dictionary containing Redshift database connection credentials.
      Requires keys 'host', 'port', 'user', 'password', and 'database'.
    - schema_name (str): Name of the SQL schema where the table resides.

    Returns:
    - None: The function does not return anything.

    Note:
    - This function relies on the 'psycopg2' library for connecting to the Redshift database.
    - The 'create_table' and 'prepare_insert_values' functions are assumed to be available in the same module.
    - The function prints "[INFO] Insert values Completed successfully" upon successful completion.

    Example:
    If filtered_tables = {'table1': DataFrame1, 'table2': DataFrame2} and
    redshift_credentials = {'host': 'your_host', 'port': 'your_port', 'user': 'your_user',
                            'password': 'your_password', 'database': 'your_database'},
    the function will insert values from DataFrame1 into 'table1' and values from DataFrame2 into 'table2'
    in the specified Redshift database.
    """
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
