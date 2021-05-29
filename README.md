# PURPOSE: 

This database has been set up to support easy-to-use optimized queries and data analysis for the Sparkify music streaming app, which stores all song and user activity data/metadata in JSON files. The ETL pipeline associated with this database extracts this data from the JSON files, transforming and loading it into a star schema defined as follows: 

## Fact Table: 
songplays : keys={songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent}

## Dimension Tables (4) 
users : keys={user_id, first_name, last_name, gender, level}\
songs : keys={song_id, title, artist_id, year, duration}\
aritsts : keys={artist_id, name, location, latitude, longitude}\
time : keys={start_time, hour, day, week, month, year, weekday}

The justifications for this choice of schema include:

a) Being denormalized, the join logic required for queries is much simpler than with a normalized schema.\
b) Simplified reporting logic for queries of business interest.\
c) Optimized query performance (especially for aggregations).


# SCRIPTS PROVIDED

1) sql_queries.py : Contains definition strings for the PostgreSQL queries to DROP all tables in the Sparkify 
                    database (if they exist), CREATE all tables required for the database, INSERT INTO all
                    tables, and to SELECT a new row to be inserted into the songplays (Fact) table.

2) create_tables.py : Establishes a connection with the Sparkify database and gets a cursor to it, runs the 
                      queries in sql_queries.py to DROP all tables (if they exist), runs the queries in
                      sql_queries.py to CREATE all necessary tables, and then closes the database connection.
                      
3) etl.py : Iterates through Sparkify JSON log and song files and extracts, transforms and loads the data into
            the tables required for the database. This includes using the SELECT query from sql_queries.py
            required to insert each new row into the songplays (Fact) table.
                      

# RUNNING THE PROVIDED SCRIPTS:

1) Drop any existing tables and create new (empty) tables:

From the terminal (in the directory containing the Python scripts), type: `python create_tables.py`

(*NOTE: This script imports sql_queries.py, which must be in the same directory*)

2) Extract the JSON files, Transform and Load data into the Fact and Dimension Tables:

From the terminal, type: `python etl.py``

(*NOTE: This script also imports sql_queries.py, which must be in the same directory*)
