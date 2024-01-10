import pymssql
import pandas as pd
from pandas import DataFrame
import logging


def _sql_server_get_connection(host: str, user: str, password: str,
                               sql_server_database: str):
    conn = pymssql.connect(server=host, user=user, password=password,
                           database=sql_server_database)
    cursor = conn.cursor()

    return cursor


def execute_query(query: str, host: str, user: str, password: str,
                  sql_server_database: str) -> DataFrame:
    conn = _sql_server_get_connection(host, user, password, sql_server_database)

    try:
        conn.execute(query)
        results = conn.fetchall()
        columns = [column[0] for column in conn.description]

        data = pd.DataFrame(results, columns=columns)
    except TypeError:
        data = None
    except Exception as E:
        logging.error(Exception)
        conn.close()
        raise
    finally:
        conn.close()
    return data
