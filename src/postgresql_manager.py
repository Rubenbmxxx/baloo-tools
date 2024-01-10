import psycopg2
import pandas as pd
from baloo_tools.aws_secret_manager import get_secret_key_value


def build_query(filepath: str, params: list = None):
    """
    Build a query using the relative path from MWAA adding params if needed

    :param filepath: AWS Secret Manager file path.
    :param params: list. params to add to the query, defaults to None.
    """
    if params is None:
        params = []

    with open(filepath) as f:
        query = f.read().format(*params)
    return query


def _get_connection(secret_name: str):
    connection = psycopg2.connect(
        host=get_secret_key_value(secret_name, key_name='postgresql_host'),
        dbname=get_secret_key_value(secret_name, key_name='postgresql_db'),
        user=get_secret_key_value(secret_name, key_name='postgresql_user'),
        password=get_secret_key_value(secret_name, key_name='postgresql_password'),
        port=get_secret_key_value(secret_name, key_name='postgresql_port')
    )
    connection.set_session(autocommit=True)
    return connection


def _execute_query(secret_name: str, query: str):
    conn = []

    try:
        # Stablish PostgreSQL connection
        conn = _get_connection(secret_name)
        cursor = conn.cursor()

        # Execute query
        result = pd.read_sql_query(query, conn)

        # Close PostgreSQL connection
        cursor.close()
        conn.close()

    except Exception as e:
        conn.close()
        raise Exception(f"Error executing query: {e}")
    return result


def execute_query(secret_name: str, query_filepath: str, query_params: list = None):
    """
    Build and execute a query at PostgreSQL service.

    :param secret_name: AWS Secret Name. (e.g.  data/dev/vetolib )
    :param query_filepath: Query filepath where the query is stored.
            (e.g. dags/santevet_data_vetolib/vetolib/query/postgre/execute/select_organizations.sql )
    :param query_params: List of params to add to the query, defaults to None. (e.g.  ['value1', 'value2'])
    """

    query = build_query(filepath=query_filepath,
                        params=query_params)

    return _execute_query(secret_name=secret_name,
                          query=query)
