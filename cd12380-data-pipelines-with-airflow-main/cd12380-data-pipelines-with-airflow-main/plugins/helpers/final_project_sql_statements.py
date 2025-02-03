class SqlQueries:
    
    CREATE_staging_events_TABLE_SQL = ("""
   CREATE TABLE IF NOT EXISTS staging_events(
        artist  VARCHAR(270) distkey,
        auth    VARCHAR(15),
        first_name VARCHAR(30),
        gender  VARCHAR(2),
        iteminsession INTEGER,
        last_name VARCHAR(30),
        length DOUBLE PRECISION,
        level VARCHAR(15),
        location VARCHAR(50),
        method VARCHAR(5),
        page VARCHAR(50),
        registration VARCHAR(30),
        session_id INTEGER,
        song VARCHAR(200),
        status INTEGER,
        ts NUMERIC,
        user_agent VARCHAR(150),
        user_id INTEGER
    )
    """)

    CREATE_staging_songs_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        artist_id TEXT,
        artist_latitude DOUBLE PRECISION,
        artist_location VARCHAR(MAX),
        artist_longitude DOUBLE PRECISION,
        artist_name VARCHAR(MAX),
        duration NUMERIC,
        num_songs INTEGER,
        song_id TEXT distkey,
        title VARCHAR(MAX),
        year INTEGER
    )
    """)

    CREATE_songplays_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS songplays(
       songplay_id TEXT  distkey,
       start_time timestamp sortkey,
       user_id INTEGER ,
       level VARCHAR(15),
       song_id TEXT,
       artist_id VARCHAR(100),
       session_id INTEGER,
       location VARCHAR(50),
       user_agent VARCHAR(150)
    )
    """)

    CREATE_users_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS users(
       user_id INTEGER,
       first_name VARCHAR(80),
       last_name VARCHAR(80),
       gender VARCHAR(2),
       level VARCHAR(15)
    )
    """)

    CREATE_songs_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS songs(
       song_id TEXT PRIMARY KEY distkey,
       title VARCHAR(MAX),
       artist_id TEXT NOT NULL,
       year INTEGER,
       duration NUMERIC
    )
    """)

    CREATE_artists_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS artists(
       artist_id TEXT PRIMARY KEY distkey,
       name VARCHAR(MAX),
       location VARCHAR(MAX),
       latitude DOUBLE PRECISION,
       longitude DOUBLE PRECISION
    )
    """)

    CREATE_time_TABLE_SQL  = ("""
    CREATE TABLE IF NOT EXISTS time(
       start_time TIMESTAMP PRIMARY KEY sortkey,
       hour INTEGER,
       day INTEGER,
       week INTEGER,
       month INTEGER,
       year INTEGER,
       weekday VARCHAR(50)
    )
    """)


    songplay_table_insert = ("""
    INSERT INTO songplayS
        SELECT
                md5(events.session_id || events.start_time) songplay_id,
                events.start_time, 
                events.user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.session_id, 
                events.location, 
                events.user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
    INSERT INTO users
        SELECT distinct user_id, first_name, last_name, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
    INSERT INTO songs
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
    INSERT INTO artists
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
    INSERT INTO time
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)