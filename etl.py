import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Read in a JSON song file, exctract the data, transform it and 
    load the resulting data into the songs and artists Dimesnion Tables
    
    ARGUMENTS:
        cur (object)      : A psycopg2 cursor class object (allows PostgreSQL queries)
        filepath (string) : Directory path to the JSON file to be extracted
        
    RETURNS:
        None
    '''
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', \
                  'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Read in a JSON log file, exctract the data, transform it and 
    load the resulting data into the times and users Dimesnion Tables
    amd into the songplays Fact Table
    
    ARGUMENTS:
        cur (object)      : A psycopg2 cursor class object (allows PostgreSQL queries)
        filepath (string) : Directory path to the JSON file to be extracted
        
    RETURNS:
        None
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.weekofyear, \
           t.dt.month, t.dt.year, t.dt.weekday], axis=1).values.tolist()
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

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
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, \
                         songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Iterates through JSON files in filepath and uses the specified processing function
    (process_song_file or process_log_file) to extract, transform and load data into the
    PostgreSQL tables indicated by the processing function
    
    ARGUMENTS:
        cur (object)      : A psycopg2 cursor class object (allows PostgreSQL queries)
        conn (object)     : A psycopg2 connection class object (enables connection to a PostgreSQL database)
        filepath (string) : Directory path to the JSON file to be extracted
        func (object)     : A Python processing function (can be process_song_file or process_log_file)
        
    RETURNS:
        None
    '''
    
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
    '''
    - Establishes connection with the sparkify database and gets
      cursor to it.
    
    - Extracts song data JSON files, appropriately transforms the data 
      and loads it into the songs and artists Dimension Tables.
      
    - Extracts log data JSON files, appropriately transforms the data
      and loads it into the time and users Dimension Tables.
      
    - Uses transformed data from all previous extractions to load into
      the songplays Fact Table.
      
    - NOTE: create_tables.py must be run prior to running this script
    '''
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()