import os

HOST = os.environ.get("HOST")

USER_DB = os.environ.get("USER_DB")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
MYSQL_DB = os.environ.get("MYSQL_DB")
MYSQL_PORT = os.environ.get("MYSQL_PORT")

DEFAULT_CREDS = {
    "user": os.environ.get("SB_USERNAME"),
    "passwd": os.environ.get("SB_PASSWORD"),
}

COMPETITION_ID = [7, 1267]

SEASON_ID = [235, 108, 107]

OPEN_DATA_PATHS = {
    "competitions": "https://raw.githubusercontent.com/statsbomb/open-data/master/data/competitions.json",
    "matches": "https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/{competition_id}/{season_id}.json",
    "lineups": "https://raw.githubusercontent.com/statsbomb/open-data/master/data/lineups/{match_id}.json",
    "events": "https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/{match_id}.json",
    "frames": "https://raw.githubusercontent.com/statsbomb/open-data/master/data/three-sixty/{match_id}.json",
}

N_THREAD = 4