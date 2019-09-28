import os
import glob
import json
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Processes single song file and inserts the data into the database.
    
    Extract data about song and the artist from the song JSON file and inserts
    the records into the appropriate tables in the database.

    Parameters
    ----------
    cur : cursor
        Postgres python wrapper's cursor object

    filepath : str
        Path to JSON song file
    """
    # open song file
    # this method seems to work instead of read_json
    json_file = json.load(open(filepath, 'rb')) 
    df = pd.DataFrame(json_file, index=[0])

    # insert song record
    song_cols = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_cols.values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_cols = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_cols.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Processes single log file and inserts the data into the database.
    
    Extract log data from log file and inserts the records into the appropriate
    tables in the database.

    Parameters
    ----------
    cur : cursor
        Postgres python wrapper's cursor object

    filepath : str
        Path to log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = {'ts': df['ts'],'hour': t.dt.hour, 'day': t.dt.day, 'week_of_year': t.dt.weekofyear,
                 'month': t.dt.month, 'year': t.dt.year, 'weekday': t.dt.weekday}
#     column_labels = 
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        # TODO: change to a more 'Pythoninc' way of doing this.
        songplay_data = row[['ts', 'userId', 'level']].values.tolist() + [songid, artistid] + row[['sessionId', 'location', 'userAgent']].values.tolist()
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Iterates through the available files and use provided function.

    Parameters
    ----------
    cur : cursor
        Postgres python wrapper's cursor object

    conn : connection
        Postgres Python wrapper's connection to datebase object

    filepath : str
        Path to log file

    func : function
        Function to use for processing file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
