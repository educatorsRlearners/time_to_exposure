import pandas as pd
import psycopg2
import sys

sys.path.append("../secrets/")

from delta_dna import USERNAME, PASSWORD


def execute_query(query, columns):
    """Returns a dataframe of the query results

    Args:
        query (string): SQL query to be executed

    Returns:
        pd.DataFrame: Dataframe of the query results
    """
    try:
        connection = psycopg2.connect(
            user=USERNAME,  # find on 1password
            password=PASSWORD,  # find on 1password
            host="data.deltadna.net",
            port="5432",
            database="mrs-wordsmith.jupiter",
            sslmode="require",
        )
        cursor = connection.cursor()
        cursor.execute(query=query)
        events_records = cursor.fetchall()
        df = pd.DataFrame(events_records, columns=columns)
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
    return df
