import argparse
import logging
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

import mysql.connector
from sqlalchemy import types

from config import HOST, MYSQL_DB, MYSQL_PASSWORD, USER_DB, N_THREAD
from extract_data import (extract_competitions, extract_events_lineups,
                          extract_matches, update_competitions, update_matches)
from load_data import load_data, update_data
from sql_queries import (create_table_competition, create_table_events,
                         create_table_lineups, create_table_matches)
from transform_data import transform_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

connection_params = {"host": HOST, "user": USER_DB, "password": MYSQL_PASSWORD}

db_connection_params = {
    "host": HOST,
    "user": USER_DB,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DB,
}


def main():
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logging.basicConfig(filename="etl.log", level=logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update", action=argparse.BooleanOptionalAction, required=True
    )

    args = parser.parse_args()

    try:
        logger.info("database being created")

        mydb = mysql.connector.connect(**connection_params)
        mycursor = mydb.cursor()
        query = "CREATE DATABASE IF NOT EXISTS " + MYSQL_DB
        mycursor.execute(query)

        logger.info("table being created")

        mydb = mysql.connector.connect(**db_connection_params)
        mycursor = mydb.cursor()
        mycursor.execute(create_table_competition)
        mycursor.execute(create_table_matches)
        mycursor.execute(create_table_lineups)
        mycursor.execute(create_table_events)
    except Exception as e:
        logger.critical(f"The creation of the database/tables failed - {e}")

    logger.info("data extraction")

    if args.update == False:
        try:
            df_competitions = extract_competitions()
            df_matches = extract_matches(df_competitions=df_competitions)
            
            match_ids = list(df_matches["match_id"].unique())

            pool = ThreadPool(N_THREAD)
            pool.map(extract_events_lineups, match_ids)
            pool.close()
            pool.join()

            logger.info("data transformation")
            df_competition, df_matches, df_lineups, df_events = transform_data()

            logger.info("data loading")
            load_data(df=df_competition, table_name="competition")
            load_data(df=df_matches, table_name="matches")
            load_data(df=df_lineups, table_name="lineups")
            
            match_ids = list(df_events["match_id"].unique())
            df_events_list = [df_events.loc[df_events['match_id']==match_id] for match_id in match_ids]

            pool = ThreadPool(N_THREAD)
            pool.map(partial(load_data, table_name="events"), df_events_list)
            pool.close()
            pool.join()
        except Exception as e:
            logger.critical(f"Data loading failed - {e}")
    else:
        try:
            df_competitions_to_update = update_competitions()
            df_matches_to_update = update_matches(
                df_competitions_to_update=df_competitions_to_update
            )
            match_ids_to_update = list(df_matches_to_update["match_id"])
            pool = ThreadPool(N_THREAD)
            pool.map(extract_events_lineups, match_ids_to_update)
            pool.close()
            pool.join()

            logger.info("data transformation")
            df_competition, df_matches, df_lineups, df_events = transform_data()

            df_matches_filtered = df_matches.loc[
                df_matches["match_id"].isin(match_ids_to_update)
            ]
            df_lineups_filtered = df_lineups.loc[
                df_lineups["match_id"].isin(match_ids_to_update)
            ]
            df_events_filtered = df_events.loc[
                df_events["match_id"].isin(match_ids_to_update)
            ]

            logger.info("data loading")
            update_data(
                df=df_competition,
                table_name="competition",
                col_types={
                    "competition_id": types.INTEGER(),
                    "season_id": types.INTEGER(),
                    "country_name": types.VARCHAR(length=50),
                    "competition_name": types.VARCHAR(length=50),
                    "competition_gender": types.VARCHAR(length=50),
                    "competition_youth": types.BOOLEAN,
                    "competition_international": types.BOOLEAN(),
                    "season_name": types.VARCHAR(length=50),
                    "match_updated": types.DATETIME(),
                    "match_updated_360": types.DATETIME(),
                    "match_available_360": types.DATETIME(),
                    "match_available": types.DATETIME(),
                },
            )
            load_data(df=df_matches_filtered, table_name="matches")
            load_data(df=df_lineups_filtered, table_name="lineups")
            df_events_list = [df_events_filtered.loc[df_events_filtered['match_id']==match_id] for match_id in match_ids_to_update]

            pool = ThreadPool(N_THREAD)
            pool.map(partial(load_data, table_name="events"), df_events_list)
            pool.close()
            pool.join()
        except Exception as e:
            logger.critical(f"Data updating failed - {e}")

    mydb.close()
    logger.info("end of the etl")


if __name__ == "__main__":
    main()
