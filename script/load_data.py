import pandas as pd
from sqlalchemy import create_engine

from config import HOST, MYSQL_DB, MYSQL_PASSWORD, MYSQL_PORT, USER_DB


def load_data(
    df: pd.DataFrame,
    table_name: str,
    user: str = USER_DB,
    password: str = MYSQL_PASSWORD,
    host: str = HOST,
    port: int = MYSQL_PORT,
    db: str = MYSQL_DB,
) -> None:
    """function to load the transformed data to the mySQL database

    Args:
        df (pd.DataFrame): transformed data to load in the table of the database
        table_name (str): the name of the table
        user (str, optional): mySQL user. Defaults to USER.
        password (str, optional): mySQL password. Defaults to MYSQL_PASSWORD.
        host (str, optional): host of the database. Defaults to HOST.
        port (int, optional): port of the database. Defaults to MYSQL_PORT.
        db (str, optional): the name of the database. Defaults to MYSQL_DB.
    """
    connection = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    )
    df.to_sql(name=table_name, con=connection, if_exists="append", index=False, chunksize=10000)


def update_data(
    df: pd.DataFrame,
    table_name: str,
    col_types: dict,
    user: str = USER_DB,
    password: str = MYSQL_PASSWORD,
    host: str = HOST,
    port: int = MYSQL_PORT,
    db: str = MYSQL_DB,
) -> None:
    """function to load the transformed data to the mySQL database

    Args:
        df (pd.DataFrame): transformed data to load in the table of the database
        table_name (str): the name of the table
        user (str, optional): mySQL user. Defaults to USER.
        password (str, optional): mySQL password. Defaults to MYSQL_PASSWORD.
        host (str, optional): host of the database. Defaults to HOST.
        port (int, optional): port of the database. Defaults to MYSQL_PORT.
        db (str, optional): the name of the database. Defaults to MYSQL_DB.
    """
    connection = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    )
    df.to_sql(
        name=table_name,
        con=connection,
        if_exists="replace",
        index=False,
        dtype=col_types,
    )
