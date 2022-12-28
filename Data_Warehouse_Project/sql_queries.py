import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_events (
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
                              CREATE TABLE IF NOT EXISTS staging_songs (
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
                           artist_id VARCHAR NOT NULL SORTKEY,
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
                       COPY staging_events FROM {}
                       credentials 'aws_iam_role={}'
                       format as json {}
                       STATUPDATE ON
                       region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
                      COPY staging_songs FROM {}
                      credentials 'aws_iam_role={}'
                      format as json 'auto'
                      ACCEPTINVCHARS AS '^'
                      STATUPDATE ON
                      region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
                         INSERT INTO songplays (
                             start_time,
                             user_id,
                             level,
                             song_id,
                             artist_id,
                             session_id,
                             location,
                             user_agent
                         )
                         SELECT DISTINCT se.ts,
                            se.userId,
                            se.level,
                            ss.song_id,
                            ss.artist_id,
                            se.sessionId,
                            se.location,
                            se.userAgent
                         FROM staging_events AS se JOIN staging_songs AS ss 
                            ON se.song == ss.title AND se.artist == ss.artist_name
                         WHERE se.page = 'NextSong';                      
""")

user_table_insert = ("""
                     INSERT INTO users (
                         user_id,
                         first_name,
                         last_name,
                         gender,
                         level
                     )
                     SELECT DISTINCT se.userId,
                        se.firstName,
                        se.lastName,
                        se.gender,
                        se.level
                     FROM staging_events AS se
                     WHERE se.userId IS NOT NULL;
""")

song_table_insert = ("""
                     INSERT INTO songs(
                         song_id,
                         title,
                         artist_id,
                         year,
                         duration
                     )
                     SELECT DISTINCT  ss.song_id,
                        ss.title,
                        ss.artist_id,
                        ss.year,
                        ss.duration
                     FROM staging_songs AS ss
                     WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
                       INSERT INTO artists (
                           artist_id,
                           name,
                           location,
                           latitude,
                           logitude
                       )
                       SELECT DISTINCT ss.artist_id, 
                        ss.artist_name, 
                        ss.artist_location,
                        ss.artist_latitude,
                        ss.artist_longitude
                       FROM staging_songs AS ss
                       WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
                     INSERT INTO time (
                         start_time,
                         hour,
                         day,
                         week,
                         month,
                         year,
                         weekday
                     )
                     SELECT DISTINCT se.ts,
                        EXTRACT(hour FROM se.ts),
                        EXTRACT(day FROM se.ts),
                        EXTRACT(week FROM se.ts),
                        EXTRACT(month FROM se.ts),
                        EXTRACT(year FROM se.ts),
                        EXTRACT(weekday FROM se.ts)
                     FROM staging_events AS se
                     WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert]
