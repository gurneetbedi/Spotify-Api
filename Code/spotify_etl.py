from sqlalchemy.orm import sessionmaker
import sqlalchemy
import requests
import json
from datetime import datetime, time
import datetime
import sqlite3
import pandas as pd
from code_taken import TOKEN, base_64,  refresh_token
# from check_data import check_if_valid_data


class Refresh:

    def __init__(self):
        self.refresh_token = refresh_token
        self.base64 = base_64

    def refresh(self):
        query = "https://accounts.spotify.com/api/token"

        response = requests.post(query,
                                 data={"grant_type": "refresh_token",
                                       "refresh_token": refresh_token},
                                 headers={"Authorization": "Basic " + base_64})

        response_json = response.json()
        return response_json['access_token']


a = Refresh()
new_token = a.refresh()


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if DF is empty
    if df.empty:
        print('No songs Downloaded. Finishing Execution')
        return False
    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Check Key Is violated")
    # Check For Nulls
    if df.isnull().values.any():
        raise Exception("Null Value Found")
    # Check All time stamps are from yesterdays data
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['timestamp'].tolist()

    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception(
                "At least one of the returned songs is not from within the last 24 hrs")

    return True


def run_spotify_etl():
    database_location = 'sqlite:///my_played_tracks.sqlite'
    user_id = 'aphcm8592zjnjfo1k965kd1iz'
    token = new_token

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(
        time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    # print(data)
    song_names = []
    artist_names = []
    played_at_list = []
    timestamp = []

    for song in data['items']:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamp.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamp
    }

    song_df = pd.DataFrame(song_dict, columns=[
                           "song_name", "artist_name", "played_at", "timestamp"])

    # print(song_df)
    # Validate
    if check_if_valid_data(song_df):
        print("Data Valid Proceed to Load Stage")

    # Load

    engine = sqlalchemy.create_engine(database_location)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine,
                       index=False, if_exists='append')
    except Exception as e:
        print("There was an issue loading the data. " + str(e))

    conn.close()
    print("Close database successfully")
