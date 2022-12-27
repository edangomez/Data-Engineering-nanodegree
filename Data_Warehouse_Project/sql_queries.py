import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                              CREATE IF NOT EXISTS staging_events (
                                  event_id BIGINT IDENTITY(0,1),
                                  artist VARCHAR,
                                  auth VARCHAR,
                                  firstName VARCHAR,
                                  gender VARCHAR,
                                  itemInSession VARCHAR,
                                  lasName VARCHAR,
                                  length VARCHAR,
                                  level VARCHAR,
                                  location VARCHAR,
                                  method VARCHAR,
                                  page VARCHAR,
                                  registration VARCHAR,
                                  sessionId INTEGER SORTKEY DISTKEY,
                                  song VARCHAR,
                                  status INTEGER,
                                  ts BIGINT,
                                  userAgent VARCHAR,
                                  userId INTEGER
                              );
""")

staging_songs_table_create = ("""
                              CREATE IF NOT EXISTS staging_songs (
                                  num_songs INTEGER,
                                  artist_id VARCHAR SORTKEY DISTKEY,
                                  artist_latitude VARCHAR,
                                  artist_longitude VARCHAR,
                                  artist_location VARCHAR(500),
                                  artist_name VARCHAR(500),
                                  song_id VARCHAR,
                                  title VARCHAR(500),
                                  duration DECIMAL(9),
                                  year INTEGER
                              );
""")

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays (
                             songplay_id INTEGER IDENTITY (0,1) NOT NULL SORTKEY,
                             start_time TIMESTAMP NOT NULL,
                             user_id INTEGER NOT NULL DISTKEY,
                             level VARCHAR NOT NULL,
                             song_id VARCHAR NOT NULL,
                             artist_id VARCHAR NOT NULL,
                             session_id VARCHAR NOT NULL,
                             location VARCHAR NULL,
                             user_agent VARCHAR NULL
                         );
""")

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users (
                         user_id INTEGER NOT NULL SORTKEY,
                         first_name VARCHAR NULL,
                         last_name VARCHAR NULL,
                         gender VARCHAR NULL,
                         level VARCHAR NULL
                     );
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs (
                         song_id VARCHAR NOT NULL SORTKEY,
                         title VARCHAR NOT NULL,
                         artist_id VARCHAR NOT NULL,
                         year INTEGER NOT NULL,
                         duration DECIMAL NOT NULL
                     );
""")

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists (
                           artist_id VARCHAR NOT NULL SORTKEY.
                           name VARCHAR NOT NULL,
                           location VARCHAR NOT NULL,
                           latitude VARCHAR NOT NULL,
                           longitude VARCHAR NOT NULL
                       );
""")

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time (
                         start_time TIMESTAMP NOT NULL SORTKEY,
                         hour NUMERIC NOT NULL,
                         day NUMERIC NOT NULL,
                         week NUMERIC NOT NULL,
                         month NUMERIC NOT NULL,
                         year NUMERIC NOT NULL,
                         weekday NUMERIC NOT NULL             
                     );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
