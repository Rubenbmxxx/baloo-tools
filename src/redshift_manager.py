import psycopg2
import pandas as pd
from baloo_tools.aws_secret_manager import get_secret_key_value
import warnings


def _get_connection(secret_name):
    connection = psycopg2.connect(
        host=get_secret_key_value(secret_name, key_name='redshift_host'),
        dbname=get_secret_key_value(secret_name, key_name='redshift_db'),
        user=get_secret_key_value(secret_name, key_name='redshift_user'),
        password=get_secret_key_value(secret_name, key_name='redshift_password'),
        port=get_secret_key_value(secret_name, key_name='redshift_port')
    )
    connection.set_session(autocommit=True)
    return connection


def build_query(filepath: str, params: list = None):
    """
    Build a query using the relative path from MWAA adding params if needed

    :param filepath: AWS Secret Manager.
    :param params: list. params to add to the query, defaults to None.
    """
    if params is None:
        params = []

    with open(filepath) as f:
        queries = f.read().format(*params)
    return queries


def execute_query(secret_name: str, query_file: str, params: list = None):
    """
    Executes the query file script parameterizing it with 'params' argument

    :param secret_name: AWS Secret Manager. e.g.(data/digivet)
    :param query_file: Query file path. e.g.('dags/santevet_data_digivet/digivet_docs_google/query/
    redshift/copy/copy_vetstoria.sql')
    :param params: list. params to add to the query, defaults to None. e.g.(['zzz1', 'zzz2'])
    """
    try:
        query = build_query(filepath=query_file,
                            params=params)

        connection = _get_connection(secret_name)
        cursor = connection.cursor()
        cursor.execute(query)

        # Commit the changes and close the cursor and connection
        connection.commit()
        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        return f"Error: Unable to connect to the database. {e}"
        raise
    except Exception as e:
        return f"Error: An unexpected error occurred. {e}"
        raise


def _execute_query_df(secret_name: str, query: str):

    conn = []

    try:
        # Establish connection
        conn = _get_connection(secret_name)
        # Execute query
        result = pd.read_sql_query(query, conn)
    except TypeError:
        result = None
    except:
        conn.close()
        raise
    finally:
        conn.close()
    return result


def execute_query_df(secret_name: str, query_filepath: str, query_params: list = None):
    """
    Build and execute a query at Redshift.

    :param secret_name: AWS Secret Name. (e.g.  data/dev/vetolib )
    :param query_filepath: Query filepath where the query is stored.
            (e.g. dags/santevet_data_vetolib/vetolib/query/postgre/execute/select_organizations.sql )
    :param query_params: List of params to add to the query, defaults to None. (e.g.  ['value1', 'value2'])
    """

    query = build_query(filepath=query_filepath,
                        params=query_params)

    return _execute_query_df(secret_name=secret_name,
                             query=query)