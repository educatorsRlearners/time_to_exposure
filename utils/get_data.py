import sys
import numpy as np
import pandas as pd

from config import FULL_WORD_INFO_COLS, GAMEPLAY_COLUMNS

# Add secrets folder to path
sys.path.append("../secrets/")
from neo4j import GraphDatabase
from neo4j_secrets import URI, USERNAME, PASSWORD

# Add utils folder to path
sys.path.append("../utils/")
from get_delta_dna_data import execute_query
from get_neo4j_data import Neo4jConnection


def get_delta_dna(fname, columns):
    """Returns a dataframe of the game play data
    Args:
        fname (str): Name of the file to be read
        columns (list): List of column names
    Returns:
        pd.DataFrame: Dataframe of the game play data
    """
    file_path = "".join(["sql/", fname, ".sql"])
    with open(file_path, "r") as f:
        query = f.read()
        df = execute_query(query, columns)
    return df


def get_game_play_data():
    gameplay_columns = GAMEPLAY_COLUMNS

    fname = "game_play_3.6+"

    return get_delta_dna(fname, gameplay_columns)


def get_reading_age_data():
    reading_age_cols = ["childid", "reading_age"]
    fname = "reading_age_3.6+"

    df_reading_age = get_delta_dna(fname, reading_age_cols)

    df_reading_age = df_reading_age.sort_values(
        by=["childid", "reading_age"], ascending=False
    )
    # Get the highest reading age for each child
    return df_reading_age.drop_duplicates(subset="childid", keep="first")


def get_neo4j_data(columns=FULL_WORD_INFO_COLS):
    conn = Neo4jConnection(uri=URI, user=USERNAME, pwd=PASSWORD)

    columns = columns

    # Read neo4j.sql fiile and execute query
    filepath = "sql/word_info.sql"

    with open(filepath, "r") as f:
        query = f.read()
        df = pd.DataFrame(conn.query(query), columns=columns)

    # Close connection
    conn.close()

    # change the syllable column to half the length of the list
    df["syllables"] = df["syllables"].apply(lambda x: int(len(x) / 2))

    return df


def create_data_set():
    df_game_play = get_game_play_data()
    df_reading_age = get_reading_age_data()
    df_neo4j = get_neo4j_data()

    df = pd.merge(df_game_play, df_reading_age, on="childid", how="inner")
    df = df.merge(df_neo4j, left_on="pnsword", right_on="word_id", how="inner")

    df = df.drop(columns=["word_id"])

    return df


def get_pivoted(df):
    indexes = ["childid", "reading_age", "pnsword", "word", "age_of_acq", "lexile"]
    columns = {
        1: "err_1",
        2: "err_2",
        3: "err_3",
        4: "err_4",
        5: "err_5",
        6: "err_6",
        7: "err_7",
        8: "err_8",
    }

    pivoted = (
        df.pivot_table(
            index=indexes, columns="exposures", values="pnsmistakes", aggfunc="sum"
        )
        .reset_index()
        .dropna()
        .rename(columns=columns)
    )

    first_seen = (
        df.groupby(["childid", "pnsword"])["local_timestamp"].min().reset_index()
    )

    # merge the first eventtimestamp with the pivoted dataframe
    df_pivoted = pivoted.merge(first_seen, on=["childid", "pnsword"], how="left")
    df_pivoted = df_pivoted.sort_values(
        by=["childid", "reading_age", "local_timestamp"], ascending=True
    )

    df_pivoted["order"] = df_pivoted.groupby("childid").cumcount() + 1

    return df_pivoted
