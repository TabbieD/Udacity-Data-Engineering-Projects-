import configparser

# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
log_data_json = 's3://udacity-dend/log_json_path.json'
DWH_ROLE_ARN = config.get('CLUSTER', 'dwh_role_arn')

# DROP TABLES
staging_events_table_drop = "DROP TABle IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char(1),
        itemInSession integer,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        sessionId integer NOT NULL SORTKEY DISTKEY,
        song varchar,
        status integer,
        ts bigint,
        userAgent varchar,
        userId integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs integer,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year integer
        );
""")

songplay_table_create = ("""CREATE TABLE songplays (
    songplay_id integer IDENTITY(1, 1) PRIMARY KEY,
    start_time TIMESTAMP sortkey distkey,
    user_id integer,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id integer,
    location varchar,
    user_agent varchar
    );
""")

user_table_create = ("""CREATE TABLE users (
    user_id integer PRIMARY KEY sortkey,
    first_name varchar,
    last_name varchar,
    gender char(1),
    level varchar
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar PRIMARY KEY sortkey,
        title varchar,
        artist_id varchar,
        year integer,
        duration numeric
        );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar PRIMARY KEY sortkey, 
        name varchar, 
        location varchar, 
        lattitude float, 
        longitude float
        );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
        hour integer, 
        day integer, 
        week integer, 
        month integer, 
        year integer, 
        weekday varchar(9) encode bytedict
        );
""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json '{}';
""").format(DWH_ROLE_ARN, log_data_json)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto';
""").format(DWH_ROLE_ARN)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se, staging_songs ss 
    WHERE se.page = 'NextSong' AND
    se.song = ss.title AND 
    se.artist = ss.artist_name;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT 
        distinct userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT 
        DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as start_time,
        EXTRACT(HOUR FROM start_time) as hour,
        EXTRACT(DAY FROM start_time) as day,
        EXTRACT(WEEK FROM start_time) as week,
        EXTRACT(MONTH FROM start_time) as month,
        EXTRACT(YEAR FROM start_time) as year,
        to_char(start_time, 'Day') as weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
