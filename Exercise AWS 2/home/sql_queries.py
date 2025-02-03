import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stg_events(
    se_artist          VARCHAR(270) distkey,
    se_auth            VARCHAR(15) ,
    se_first_name      VARCHAR(30) ,
    se_gender          VARCHAR(2) ,
    se_iteminsession   INTEGER ,
    se_last_name       VARCHAR(30) ,
    se_length          DOUBLE PRECISION ,
    se_level           VARCHAR(15) ,
    se_location        VARCHAR(50) ,
    se_method          VARCHAR(5) ,
    se_page            VARCHAR(50) ,
    se_registration    VARCHAR(30) ,
    se_session_id      INTEGER ,
    se_song            VARCHAR(200) ,
    se_status          INTEGER ,
    se_ts              TIMESTAMP ,
    se_user_agent      VARCHAR(150) ,
    se_user_id         INTEGER 
    )""")

staging_songs_table_create = ("""
CREATE TABLE stg_songs(
    artist_id        TEXT,
    artist_latitude  FLOAT ,
    artist_location  VARCHAR(MAX) ,
    artist_longitude FLOAT ,   
    artist_name      VARCHAR(MAX) ,
    duration         NUMERIC ,
    num_songs        INTEGER ,
    song_id          TEXT distkey,
    title            VARCHAR(MAX),
    year             INTEGER
)""")

songplay_table_create = ("""
CREATE TABLE fact_songplay(
    sp_songplay_id       INTEGER IDENTITY(1,1) PRIMARY KEY distkey,
    sp_start_time        timestamp NOT NULL sortkey,
    sp_user_id           INTEGER NOT NULL,
    sp_level             VARCHAR(15) ,
    sp_song_id           TEXT NOT NULL,
    sp_artist_id         VARCHAR(100) NOT NULL,
    sp_session_id        INTEGER NOT NULL,
    sp_location          VARCHAR(50) ,
    sp_user_agent        VARCHAR(150) 
)""")

user_table_create = ("""
CREATE TABLE dim_user(
    u_user_id          INTEGER PRIMARY KEY,
    u_first_name       VARCHAR(80) ,
    u_last_name         VARCHAR(80) ,
    u_gender           VARCHAR(2) ,
    u_level            VARCHAR(15) 
)""")

song_table_create = ("""
CREATE TABLE dim_song(
    s_song_id        TEXT PRIMARY KEY distkey,
    s_title          VARCHAR(MAX) ,
    s_artist_id      TEXT NOT NULL,
    s_year           INTEGER ,
    s_duration       NUMERIC 
)""")

artist_table_create = ("""
CREATE TABLE dim_artist(
    a_artist_id      TEXT PRIMARY KEY distkey,
    a_name           VARCHAR(MAX) ,
    a_location       VARCHAR(MAX) ,
    a_latitude       FLOAT ,
    a_longitude      FLOAT 
)""")

time_table_create = ("""
CREATE TABLE dim_time(
    t_start_time     TIMESTAMP PRIMARY KEY sortkey,
    t_hour           INTEGER,
    t_day            INTEGER,
    t_week           INTEGER,
    t_month          INTEGER,
    t_year           INTEGER,
    t_weekday        VARCHAR(50)
)""")

# STAGING TABLES

staging_events_copy = ("""
COPY stg_events
FROM {}
iam_role {}
json {}
timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""copy stg_songs 
from {} 
IAM_ROLE {}
JSON 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO fact_songplay (sp_start_time,sp_user_id,sp_level,sp_song_id,sp_artist_id,sp_session_id,sp_location,sp_user_agent)
SELECT distinct se_ts,se_user_id,se_level,song_id,artist_id,se_session_id,se_location,se_user_agent
FROM stg_events se
join stg_songs ss on se.se_song=ss.title
where se.se_page='NextSong'
""")

user_table_insert = ("""INSERT INTO dim_user (u_user_id,u_first_name,u_last_name,u_gender,u_level)
SELECT distinct se_user_id,se_first_name,se_last_name,se_gender,se_level
FROM stg_events se where se_page='NextSong' and se_user_id is not null
""")

song_table_insert = ("""INSERT INTO dim_song (s_song_id,s_title,s_artist_id,s_year,s_duration)
SELECT distinct song_id,title,artist_id,year,duration
FROM stg_songs 
""")

artist_table_insert = ("""INSERT INTO dim_artist (a_artist_id,a_name,a_location,a_latitude,a_longitude)
SELECT distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude
FROM stg_songs 
""")

time_table_insert = ("""INSERT INTO dim_time (t_start_time,t_hour,t_day,t_week,t_month,t_year,t_weekday)
SELECT distinct sp_start_time,EXTRACT(hour from sp_start_time),EXTRACT(day from sp_start_time),EXTRACT(week from sp_start_time),EXTRACT(month from sp_start_time),EXTRACT(year from sp_start_time),EXTRACT(dow from sp_start_time)
FROM fact_songplay 
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
