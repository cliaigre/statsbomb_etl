import argparse
import os
import pathlib
from multiprocessing.dummy import Pool as ThreadPool

import pandas as pd

from config import COMPETITION_ID, DEFAULT_CREDS, OPEN_DATA_PATHS, SEASON_ID, N_THREAD
from utils import explode_nested_columns, get_resource

PATH = pathlib.Path(__file__).parent


folders = [
    "raw_data",
    "raw_data/competition/",
    "raw_data/matches/",
    "raw_data/lineups/",
    "raw_data/events/",
]


for folder in folders:
    if not os.path.exists(PATH.joinpath(folder)):
        os.makedirs(PATH.joinpath(folder))


def extract_competitions(
    competition_ids: list = COMPETITION_ID, season_ids: list = SEASON_ID
) -> pd.DataFrame:
    """function to extract Statsbomb competitions data and save it in the folder raw_data

    Args:
        competition_ids (list, optional): competition ids to process. Defaults to COMPETITION_ID.
        season_ids (list, optional): season ids to process. Defaults to SEASON_ID.

    Returns:
        pd.DataFrame: competitions data
    """
    competition_data = get_resource(
        OPEN_DATA_PATHS["competitions"], creds=DEFAULT_CREDS
    )
    df_competitions = pd.DataFrame(competition_data)
    df_competitions.to_feather(
        PATH.joinpath("raw_data/competition/competition.feather")
    )

    df_competitions_filtered = df_competitions.loc[
        (df_competitions["competition_id"].isin(competition_ids))
        & (df_competitions["season_id"].isin(season_ids))
    ]

    return df_competitions_filtered


def update_competitions(
    competition_ids: list = COMPETITION_ID, season_ids: list = SEASON_ID
) -> pd.DataFrame:
    """function to extract Statsbomb competitions data and save it in the folder raw_data
       return the competitions to update

    Args:
        competition_ids (list, optional): competition ids to process. Defaults to COMPETITION_ID.
        season_ids (list, optional): season ids to process. Defaults to SEASON_ID.

    Returns:
        pd.DataFrame: competitions data to update 
    """
    competition_data = get_resource(
        OPEN_DATA_PATHS["competitions"], creds=DEFAULT_CREDS
    )
    df_competitions_api = pd.DataFrame(competition_data)
    df_competitions_api_filtered = df_competitions_api.loc[
        (df_competitions_api["competition_id"].isin(competition_ids))
        & (df_competitions_api["season_id"].isin(season_ids))
    ]
    df_competitions = pd.read_feather("raw_data/competition/competition.feather")
    df_competitions_filtered = df_competitions.loc[
        (df_competitions["competition_id"].isin(competition_ids))
        & (df_competitions["season_id"].isin(season_ids))
    ]
    df_competitions_api_filtered = df_competitions_api_filtered.loc[
        ~df_competitions_api_filtered["match_available"].isna()
    ]
    df_competitions_filtered = df_competitions_filtered.loc[
        ~df_competitions_filtered["match_available"].isna()
    ]
    df_competitions_filtered = df_competitions_filtered.rename(
        columns={"match_updated": "match_updated_old"}
    )
    df_competitions_filtered = df_competitions_filtered.loc[
        :, ["competition_id", "season_id", "match_updated_old"]
    ]
    df_date_comparison_filtered = pd.merge(
        df_competitions_api_filtered,
        df_competitions_filtered,
        on=["competition_id", "season_id"],
        how="left",
    )
    df_competitions_to_update = df_date_comparison_filtered.loc[
        df_date_comparison_filtered["match_updated"]
        != df_date_comparison_filtered["match_updated_old"]
    ]
    df_competitions_api.to_feather(
        PATH.joinpath("raw_data/competition/competition.feather")
    )

    return df_competitions_to_update


def extract_matches(df_competitions: pd.DataFrame) -> pd.DataFrame:
    """function to extract Statsbomb matches data and save it in the folder raw_data

    Args:
        df_competitions_to_update (pd.DataFrame): competitions data from Statsbomb

    Returns:
        pd.DataFrame: matches data
    """
    df_all_matches = pd.DataFrame()

    for _, competition in df_competitions.iterrows():
        competition_id = competition["competition_id"]
        season_id = competition["season_id"]
        matches_data = get_resource(
            OPEN_DATA_PATHS["matches"].format(
                competition_id=competition_id, season_id=season_id
            ),
            creds=DEFAULT_CREDS,
        )
        df_matches_processed_tmp = pd.json_normalize(matches_data)
        df_matches_processed_tmp = explode_nested_columns(
            df_matches_processed_tmp, "home_team.managers"
        )
        df_matches_processed_tmp = explode_nested_columns(
            df_matches_processed_tmp, "away_team.managers"
        )
        df_matches_processed_tmp.columns = df_matches_processed_tmp.columns.str.replace(
            ".", "_"
        )
        df_matches_processed_tmp.to_feather(
            PATH.joinpath(
                f"raw_data/matches/matches_{competition_id}_{season_id}.feather"
            )
        )
        df_all_matches = pd.concat([df_all_matches, df_matches_processed_tmp])

    return df_all_matches


def update_matches(df_competitions_to_update: pd.DataFrame) -> pd.DataFrame:
    """function to extract Statsbomb matches data and save it in the folder raw_data
    return the matches to update

    Args:
        df_competitions_to_update (pd.DataFrame): competitions data from Statsbomb

    Returns:
        pd.DataFrame: matches data to update
    """

    df_all_matches_to_update = pd.DataFrame(
        columns=[
            "match_id",
            "match_date",
            "kick_off",
            "competition",
            "season",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "match_status",
            "match_status_360",
            "last_updated",
            "last_updated_360",
            "metadata",
            "match_week",
            "competition_stage",
            "stadium",
            "referee",
        ]
    )

    for _, competition in df_competitions_to_update.iterrows():
        competition_id = competition["competition_id"]
        season_id = competition["season_id"]
        matches_data_api = get_resource(
            OPEN_DATA_PATHS["matches"].format(
                competition_id=competition_id, season_id=season_id
            ),
            creds=DEFAULT_CREDS,
        )
        df_matches_processed_tmp_api = pd.json_normalize(matches_data_api)
        df_matches_processed_tmp_api = explode_nested_columns(
            df_matches_processed_tmp_api, "home_team.managers"
        )
        df_matches_processed_tmp_api = explode_nested_columns(
            df_matches_processed_tmp_api, "away_team.managers"
        )
        df_matches_processed_tmp_api.columns = df_matches_processed_tmp_api.columns.str.replace(
            ".", "_"
        )
        df_matches_processed_tmp_api_filtered = df_matches_processed_tmp_api.loc[
            (df_matches_processed_tmp_api["match_status"] == "available")
        ]
        df_matches_processed = pd.read_feather(
            PATH.joinpath(
                f"raw_data/matches/matches_{competition_id}_{season_id}.feather"
            )
        )
        df_matches_processed_filtered = df_matches_processed.loc[
            (df_matches_processed["match_status"] == "available")
        ]
        df_matches_processed_filtered = df_matches_processed_filtered.rename(
            columns={"last_updated": "last_updated_old"}
        )
        df_matches_processed_filtered = df_matches_processed_filtered.loc[
            :, ["match_id", "last_updated_old"]
        ]
        df_date_comparison = pd.merge(
            df_matches_processed_tmp_api_filtered,
            df_matches_processed_filtered,
            on=["match_id"],
            how="left",
        )
        df_date_comparison_filtered = df_date_comparison.loc[
            df_date_comparison["last_updated"] != df_date_comparison["last_updated_old"]
        ]
        df_all_matches_to_update = pd.concat(
            [df_all_matches_to_update, df_date_comparison_filtered]
        )

    return df_all_matches_to_update


def extract_events_lineups(match_id: str) -> None:
    """function to extract Statsbomb events and lineups data and save it in the folder raw_data

    Args:
        df_matches (pd.DataFrame): matches data
    """

    lineups_data = get_resource(
        OPEN_DATA_PATHS["lineups"].format(match_id=match_id), creds=DEFAULT_CREDS
    )
    events_data = get_resource(
        OPEN_DATA_PATHS["events"].format(match_id=match_id), creds=DEFAULT_CREDS
    )

    df_lineups_unnested = pd.DataFrame(lineups_data)
    df_lineups_unnested = explode_nested_columns(df_lineups_unnested, "lineup")
    df_lineups_unnested = explode_nested_columns(
        df_lineups_unnested, "lineup.cards"
    )
    df_lineups_unnested = explode_nested_columns(
        df_lineups_unnested, "lineup.positions"
    )
    df_events_unnested = pd.json_normalize(events_data)
    df_events_unnested = explode_nested_columns(
        df_events_unnested, "tactics.lineup"
    )
    df_events_unnested = explode_nested_columns(
        df_events_unnested, "shot.freeze_frame"
    )
    df_lineups_unnested.columns = df_lineups_unnested.columns.str.replace(".", "_")
    df_events_unnested.columns = df_events_unnested.columns.str.replace(".", "_")
    df_lineups_unnested.to_feather(
        PATH.joinpath(f"raw_data/lineups/lineups_{match_id}.feather")
    )
    df_events_unnested.to_feather(
        PATH.joinpath(f"raw_data/events/events_{match_id}.feather")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--update", action=argparse.BooleanOptionalAction, required=True
    )

    args = parser.parse_args()

    if args.update == False:
        df_competitions = extract_competitions()
        df_matches = extract_matches(df_competitions=df_competitions)
    else:
        df_competitions = update_competitions()
        df_matches = update_matches(df_competitions_to_update=df_competitions)
    match_ids = list(df_matches["match_id"].unique())
    pool = ThreadPool(N_THREAD)
    pool.map(extract_events_lineups, match_ids)
    pool.close()
    pool.join()
