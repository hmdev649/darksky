import requests
import pandas as pd
import numpy as np
import sqlite3
import pymongo
import dns

def get_api_key(path):
    '''
    Retrieves api keys from a given file
    Args:
        Path str: path to .secret file
    Returns str:
        Api key
        '''
    with open(path) as f:
        password = f.read().strip()
    return password

def get_season_data(database, season):
    '''
    Connect to a sqlite database and return match data from a particular season
    Args:
        database str: path to sqlite file
        season int: year of interest for match data
    Returns:
        pandas dataframe with match stats'''

    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute(f"""SELECT *
                 FROM Matches
                 WHERE Season = {season}
                 AND Div in ('D1', 'E0')
                 ;""")
    df = pd.DataFrame(c.fetchall())
    df.columns = [x[0] for x in c.description]
    return df

def get_weather_data(date, password, lat='52.5200', long='13.4050'):
    ''' Make a request to the Darksky api
    Args:
        date str: date to check the weather in YYYY-MM-DD format
        password str: api key for accessing the Darksky api
        lat str: latitude of location to check weather default: Berlin, Germany 52.5200
        long str: longitude of location to check weather default: Berlin, Germany 13.4050
    Returns: tuple of date, rain
        date str: date the weather checked in YYYY-MM-DD format
        rain bool: Did it rain this day'''

    time = date + 'T00:00:00'
    result = requests.get(f'https://api.darksky.net/forecast/{password}/{lat},{long}, {time}').json()

    icon = result['daily']['data'][0].get('icon')
    rain = icon == 'rain'

    return date, rain

def rain_dates(unique_dates, password):
    '''
    Creates a dataframe consisting of dates and Boolean values indicating if it rained that day

    Args:
        unique_dates list[str]: list of dates in YYYY-MM-DD format
        password str: api key for accessing the Darksky api
    Returns:
        Dataframe
    '''
    data = []
    for date in set(unique_dates):
        date, rain = get_weather_data(date, password)
        data.append({'Date': date,
                     'rain': rain,})

    return pd.DataFrame(data)

def match_rain_data(database, season, password, limit_api_calls=-1):
    '''
    Combines rain data from the darksky api with match data from a local sqlite database
    '''

    match_data = get_season_data('database.sqlite', season)

    unique_match_dates = match_data['Date'].unique()[:limit_api_calls]
    rain_days = rain_dates(unique_match_dates, password)

    return pd.merge(match_data, rain_days, on='Date')

def calculate_aggregate_stats_pandas(df):
    #Calculate Home stats
    home_df = df.copy()
    home_df['home_win'] = home_df['FTHG'] > home_df['FTAG']
    home_df['rain_home_win'] = (home_df['home_win']) & (home_df['rain'])
    home_stats = home_df.groupby(['HomeTeam']).sum()[['FTHG', 'home_win', 'rain_home_win', 'rain']]

    #Calculate Away Stats
    away_df = df.copy()
    away_df['away_win'] = away_df['FTAG'] > away_df['FTHG']
    away_df['rain_away_win'] = (away_df['away_win']) & (away_df['rain'])
    away_stats = away_df.groupby(['AwayTeam']).sum()[['FTAG', 'away_win', 'rain_away_win', 'rain']]

    #Combine Home and Away Stats and create rain win percentage
    df_ = pd.merge(home_stats, away_stats, left_index=True, right_index=True)

    df_['wins'] = df_['home_win'] + df_['away_win']
    df_['goals'] = df_['FTHG'] + df_['FTAG']
    df_['rain_win_pct'] = round((df_['rain_home_win'] + df_['rain_away_win']) / (df_['rain_y'] + df_['rain_x']),
                                ndigits=3)

    #Select only relevant columns
    agg = df_.loc[:, ['wins', 'goals', 'rain_win_pct']]

    #Add Season and Division columns
    final_df = pd.merge(agg,
                        df.groupby('HomeTeam').max()[['Season', 'Div']],
                        left_index=True,
                        right_index=True).reset_index()

    #Clean up column names
    final_df.rename(columns={'HomeTeam':'team_name',
                             'Div': 'division',
                             'Season': 'season'}, inplace=True)

    return final_df.to_dict(orient='records')

def calculate_aggregate_stats_sqlite(match_stats_df, database):

    conn = sqlite3.connect(database)
    match_stats_df.to_sql('temp_rain_stats', con=conn, if_exists='replace')
    c = conn.cursor()

    agg_query = '''
    SELECT
      HomeTeam as Club,
      Div,
      Season,
      (h_win + a_win) as W,
      (h_goals_for+a_goals_for) as GF,
      round((a_rain_win + h_rain_win) * 1.0 / (h_rain_games + a_rain_games), 2) as rain_win_pct

      FROM

      (SELECT Div, HomeTeam,
             SUM(CASE WHEN FTHG > FTAG THEN 1 ELSE 0 END) as h_win,
             SUM(CASE WHEN FTHG = FTAG THEN 1 ELSE 0 END) as h_draw,
             SUM(CASE WHEN FTHG < FTAG THEN 1 ELSE 0 END) as h_loss,
             SUM(CASE WHEN FTHG > FTAG AND rain == 1 THEN 1 ELSE 0 END) as h_rain_win,
             SUM(rain) as h_rain_games,
             SUM(FTHG) as h_goals_for,
             MAX(Div) as Div,
             MAX(Season) as Season
       FROM temp_rain_stats
       GROUP BY HomeTeam
       ORDER BY HomeTeam)

      JOIN

      (SELECT AwayTeam,
             SUM(CASE WHEN FTAG > FTHG THEN 1 ELSE 0 END) as a_win,
             SUM(CASE WHEN FTAG > FTHG AND rain == 1 THEN 1 ELSE 0 END) as a_rain_win,
             SUM(rain) as a_rain_games,
             SUM(FTAG) as a_goals_for

       FROM temp_rain_stats
       GROUP BY AwayTeam
       ORDER BY AwayTeam)

       ON (HomeTeam==AwayTeam)

       ORDER BY GF DESC'''

    c.execute(agg_query)

    df = pd.DataFrame(c.fetchall(),
                      columns=[x[0] for x in c.description])

    c.execute('''DROP TABLE temp_rain_stats''')

    return df.to_dict(orient='records')

def insert_to_atlas(atlas_user, atlas_key, cluster_name, collection_name, team_stats, return_ids=False):
    '''
    Inserts multiple documents into a specified mongodb atlas instance

    Args:
        atlas_user str: username
        altas_key str: api key for cluster
        cluster_name str: name of cluster
        collectionn_name str: collection to insert into
        team stats [dict]: list of dictionaries of stats for a soccer team
    returns:
        None
    '''
    client = pymongo.MongoClient(f'mongodb+srv://{atlas_user}:{atlas_key}@{cluster_name}.mongodb.net/test?retryWrites=true&w=majority')
    db = client.test
    collection = db[collection_name]

    result = collection.insert_many(team_stats,
                                    ordered=False)

    if return_ids:
        return result.inserted_ids
