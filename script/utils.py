import re
from glob import glob
from pathlib import Path

import pandas as pd
import requests as req


def get_resource(url: str, creds: dict) -> list:
    """function to get the data from Statsbomb

    Args:
        url (str): Statsbomb url
        creds (dict): credentials to get the non open data from Statsbomb

    Returns:
        list: Statsbomb data
    """
    auth = req.auth.HTTPBasicAuth(creds["user"], creds["passwd"])
    resp = req.get(url, auth=auth)
    if resp.status_code != 200:
        print(f"{url} -> {resp.status_code}")
        resp = []
    else:
        resp = resp.json()

    return resp


def explode_nested_columns(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """function to process nested data

    Args:
        df (pd.DataFrame): DataFrame with nested data
        col_name (str): the name of the column with nested data

    Returns:
        pd.DataFrame: DataFrame with processed data in the column in input
    """
    df = df.explode(col_name).reset_index(drop=True)
    df_col_name = pd.json_normalize(df[col_name]).add_prefix(f"{col_name}.")
    df_unnested = pd.concat([df, df_col_name], axis=1)
    df_unnested = df_unnested.drop([col_name], axis=1)

    return df_unnested


def create_dataframe_from_raw_data(folder: Path) -> pd.DataFrame:
    """function to create a DataFrame from raw data in Feather format

    Args:
        folders (str): folders containing the raw data

    Returns:
        pd.DataFrame: DataFrame with the raw data
    """
    df = pd.DataFrame()

    feather_paths = glob(folder, recursive=True)
    for feather_path in feather_paths:
        df_tmp = pd.read_feather(feather_path)
        if "matches" not in feather_path:
            df_tmp["match_id"] = re.search(r"\d+", feather_path).group(0)
        df = pd.concat([df, df_tmp])
    df = df.reset_index(drop=True)

    return df


def minutes_to_hours(time: str) -> str:
    """function to format time from %M:%s to %H:%M:%s 

    Args:
        time (str): time in %M:%s

    Returns:
        str: time in %H:%M:%s 
    """
    min = int(re.findall(r"(\d+)\:", time)[0])
    sec = int(re.findall(r"\:(\d+)", time)[0])
    h = min // 60
    m = min % 60
    time_processed = str(0) + str(h) + ":" + str(m) + ":" + str(sec)

    return time_processed


def separate_coordinates(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """function to create two columns x and y from coordinates column

    Args:
        df (pd.DataFrame): DataFrame to process
        col_name (str): the name of the column with the coordinates

    Returns:
        pd.DataFrame: DataFrame with processed data in the column in input
    """
    df.loc[~df[col_name].isna(), col_name + "_x"] = df.loc[
        ~df[col_name].isna(), col_name
    ].apply(lambda x: x[0])
    df.loc[~df[col_name].isna(), col_name + "_y"] = df.loc[
        ~df[col_name].isna(), col_name
    ].apply(lambda x: x[1])
    if col_name == "shot_end_location":
        df.loc[~df[col_name].isna(), col_name + "_z"] = df.loc[
            ~df[col_name].isna(), col_name
        ].apply(lambda x: x[2] if len(x) > 2 else None)
    df_processed = df.drop(col_name, axis=1)

    return df_processed
