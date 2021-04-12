import configparser

'''This document consists of every variables with SQL statement. It is a definition file where the SQL statements / variables are defined and imported into the ETL.py and create_tables.py files.'''

# Config referencing to dwh.cfg file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# The following are used in create_tables.py used for table drop.

staging_event_drop = "DROP TABLE IF EXISTS staging_event"
staging_song_drop = "DROP TABLE IF EXISTS staging_song"
songplay_drop = "DROP TABLE IF EXISTS songplay"
user_drop = "DROP TABLE IF EXISTS users"
song_drop = "DROP TABLE IF EXISTS song"
artist_drop = "DROP TABLE IF EXISTS artist"
time_drop = "DROP TABLE IF EXISTS time"

# The following are used to create tables used in create_tables.py.

staging_event_create = ("""
CREATE TABLE staging_event(
    event_id INT IDENTITY(0,1),
    artist VARCHAR(255),
    auth VARCHAR(255),
    first_name VARCHAR(255),
    gender VARCHAR(1),
    item_in_session INTEGER,
    last_name VARCHAR(255),
    length FLOAT, 
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(255),
    page VARCHAR(255),
    registration VARCHAR(255),
    session_id BIGINT,
    song VARCHAR(255),
    status INTEGER, 
    ts VARCHAR(255),
    user_agent VARCHAR(255),
    user_id VARCHAR(255),
    PRIMARY KEY (event_id))
""")

staging_song_create = ("""
CREATE TABLE staging_song(
    song_id INT IDENTITY(0,1),
    num_songs INTEGER,
    artist_id VARCHAR(255),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration FLOAT,
    year INTEGER,
    PRIMARY KEY (song_id))
""")

songplay_create = ("""
CREATE TABLE songplay(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP,
    user_id VARCHAR(255),
    level VARCHAR(255),
    song_id VARCHAR(255),
    artist_id VARCHAR(255),
    session_id BIGINT,
    location VARCHAR(255),
    user_agent VARCHAR(255),
    PRIMARY KEY (songplay_id))
""")

user_create = ("""
CREATE TABLE users(
    user_id VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(255),
    PRIMARY KEY (user_id))
""")

song_create = ("""
CREATE TABLE song(
    song_id VARCHAR(255),
    title VARCHAR(255),
    artist_id VARCHAR(255) NOT NULL,
    year INTEGER,
    duration FLOAT,
    PRIMARY KEY (song_id))
""")

artist_create = ("""
CREATE TABLE artist(
    artist_id VARCHAR(255),
    name VARCHAR(255),
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (artist_id))
""")

time_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time))
""")

# The following are used for loading staging table in ETL.py to be loaded onto redshift. 

staging_event_copy = ("""copy staging_event 
                          from {}
                          iam_role {}
                          json {};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSON_PATH'))

staging_song_copy = ("""copy staging_song 
                          from {} 
                          iam_role {}
                          json 'auto';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# The following are used for the final tables (fact and dimension tables) after it is loaded onto redshift from S3 bucket. 
#Timestamp conversion into start time was referenced from https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift

songplay_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    TIMESTAMP 'epoch' + event.ts/1000 * interval '1 second' as start_time, 
    event.user_id, 
    event.level, 
    song.song_id,
    song.artist_id, 
    event.session_id,
    event.location, 
    event.user_agent
FROM staging_song song, staging_event event
WHERE event.page = 'NextSong' 
AND event.song = song.title 
AND event.artist = song.artist_name 
AND event.length = song.duration
""")

user_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
FROM staging_event
WHERE page = 'NextSong'
""")

song_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_song
WHERE song_id IS NOT NULL
""")

artist_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_song
WHERE artist_id IS NOT NULL
""")

time_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM songplay
""")

# The following are query lists for create, drop, copy and insert. 

create_table_queries = [staging_event_create, staging_song_create, songplay_create, user_create, song_create, artist_create, time_create]
drop_table_queries = [staging_event_drop, staging_song_drop, songplay_drop, user_drop, song_drop, artist_drop, time_drop]
copy_table_queries = [staging_event_copy, staging_song_copy]
insert_table_queries = [songplay_insert, user_insert, song_insert, artist_insert, time_insert]