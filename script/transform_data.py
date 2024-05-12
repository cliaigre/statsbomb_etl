import pathlib

import pandas as pd

from utils import (create_dataframe_from_raw_data, minutes_to_hours,
                   separate_coordinates)

PATH = pathlib.Path(__file__).parent


def transform_data() -> pd.DataFrame:
    """function to transform feather raw data before loading into relationnal db

    Returns:
        pd.DataFrame: DataFrames containing all data (competition, matches, lineups, events) from Statsbomb
    """
    df_competition = pd.read_feather(
        PATH.joinpath("raw_data/competition/competition.feather")
    )

    df_matches = create_dataframe_from_raw_data("**/raw_data/matches/*.feather")

    df_lineups = create_dataframe_from_raw_data("**/raw_data/lineups/*.feather")
    df_lineups.loc[
        ~df_lineups["lineup_cards_time"].isna(), "lineup_cards_time"
    ] = df_lineups.loc[
        ~df_lineups["lineup_cards_time"].isna(), "lineup_cards_time"
    ].apply(
        minutes_to_hours
    )
    df_lineups.loc[~df_lineups["lineup_cards_time"].isna(), "lineup_cards_time"] = (
        df_lineups.loc[~df_lineups["lineup_cards_time"].isna(), "lineup_cards_time"]
        .apply(pd.to_datetime, format="%H:%M:%S", errors="coerce")
        .dt.time
    )
    df_lineups.loc[
        ~df_lineups["lineup_positions_from"].isna(), "lineup_positions_from"
    ] = df_lineups.loc[
        ~df_lineups["lineup_positions_from"].isna(), "lineup_positions_from"
    ].apply(
        minutes_to_hours
    )
    df_lineups.loc[
        ~df_lineups["lineup_positions_from"].isna(), "lineup_positions_from"
    ] = (
        df_lineups.loc[
            ~df_lineups["lineup_positions_from"].isna(), "lineup_positions_from"
        ]
        .apply(pd.to_datetime, format="%H:%M:%S", errors="coerce")
        .dt.time
    )
    df_lineups.loc[
        ~df_lineups["lineup_positions_to"].isna(), "lineup_positions_to"
    ] = df_lineups.loc[
        ~df_lineups["lineup_positions_to"].isna(), "lineup_positions_to"
    ].apply(
        minutes_to_hours
    )
    df_lineups.loc[~df_lineups["lineup_positions_to"].isna(), "lineup_positions_to"] = (
        df_lineups.loc[~df_lineups["lineup_positions_to"].isna(), "lineup_positions_to"]
        .apply(pd.to_datetime, format="%H:%M:%S", errors="coerce")
        .dt.time
    )

    df_events = create_dataframe_from_raw_data("**/raw_data/events/*.feather")
    df_events = df_events.rename(columns={"index": "index_event", "out": "out_event"})
    df_events.loc[~df_events["tactics_formation"].isna(), "tactics_formation"] = (
        df_events.loc[~df_events["tactics_formation"].isna(), "tactics_formation"]
        .apply(int)
        .apply(str)
    )
    df_events = df_events.explode("related_events").reset_index(drop=True)
    df_events = separate_coordinates(df=df_events, col_name="location")
    df_events = separate_coordinates(df=df_events, col_name="carry_end_location")
    df_events = separate_coordinates(df=df_events, col_name="goalkeeper_end_location")
    df_events = separate_coordinates(df=df_events, col_name="shot_end_location")
    df_events = separate_coordinates(df=df_events, col_name="pass_end_location")
    df_events = separate_coordinates(
        df=df_events, col_name="shot_freeze_frame_location"
    )

    return df_competition, df_matches, df_lineups, df_events
