import mysql.connector
import pandas as pd
import logging


def format_query(query: str, params: list):
    if params is None:
        return query
    else:
        query_formatted = query.format(*params)
        return query_formatted


def _execute_query(query: str, host: str, user: str, password: str, database: str):
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = connection.cursor()
    try:
        cursor.execute(query)

        # Fetch all the rows
        result = cursor.fetchall()

        # Get the column names
        column_names = [column[0] for column in cursor.description]

        # Create a DataFrame from the query results
        df = pd.DataFrame(result, columns=column_names)

        return df

    except mysql.connector.Error as error:
        logging.error(f"Error: {error}")

    finally:
        cursor.close()


def execute_query(query: str, host: str, user: str, password: str, database: str, params: list = None) -> pd.DataFrame:
    query = format_query(query, params)

    result = _execute_query(query, host, user, password, database)

    return result
