class SqlQueries:
    # Guess I get to rewrite all this stuff.  Awesome.
    # I'm not going to use my previous stuff since it involves
    # uploading csv files for a time and date table.
    # I'll use the inferior schema where we populate
    # the time dimension from the staging events.
    # This project is essentially a rewrite of the previous project
    # but uses airflow to orchestrate it.
    # after figuring out that i couldn't submit the workspace, i discovered
    # that there was an sql queries file in the github.  Thanks udacity,
    # you really know how to set up projects.\s
    staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id BIGINT IDENTITY(1,1) PRIMARY KEY
        , artist VARCHAR
        , auth VARCHAR
        , firstName VARCHAR
        , gender VARCHAR
        , itemInSession INT
        , lastName VARCHAR
        , length DOUBLE PRECISION
        , level VARCHAR
        , location VARCHAR
        , method VARCHAR
        , page VARCHAR
        , registration BIGINT
        , sessionId INT
        , song VARCHAR
        , status INT
        , ts BIGINT
        , userAgent VARCHAR
        , userId INT
        );
    """)

    staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_data_id BIGINT IDENTITY(1,1) PRIMARY KEY
        , num_songs INT
        , artist_id VARCHAR
        , artist_latitude DOUBLE PRECISION
        , artist_longitude DOUBLE PRECISION
        , artist_location VARCHAR(1000)
        , artist_name VARCHAR(1000)
        , song_id VARCHAR
        , title VARCHAR
        , duration DOUBLE PRECISION
        , year INT
    );
    """)

    user_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_key BIGINT IDENTITY(1,1) PRIMARY KEY
        , user_id INT
        , first_name VARCHAR
        , last_name VARCHAR
        , gender VARCHAR(1)
        , level VARCHAR
    )
    """)

    songs_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_key BIGINT IDENTITY(1,1) PRIMARY KEY
        , song_id VARCHAR
        , artist_id VARCHAR
        , title VARCHAR
        , year VARCHAR
        , duration DOUBLE PRECISION
    );
    """)
    
    artists_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_key BIGINT IDENTITY(1,1)
        , artist_id VARCHAR
        , name VARCHAR(1000)
        , location VARCHAR(1000)
        , latitude DOUBLE PRECISION
        , longitude DOUBLE PRECISION
    );
    """)

    time_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP
        , hour VARCHAR
        , day VARCHAR
        , week VARCHAR
        , month VARCHAR
        , year VARCHAR
        , weekday VARCHAR
    );
    """)

    songplay_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id VARCHAR
        , start_time TIMESTAMP
        , user_id INT
        , "level" VARCHAR
        , song_id VARCHAR
        , artist_id VARCHAR
        , session_id VARCHAR
        , location VARCHAR
        , user_agent VARCHAR
    );
    """)

    songplay_table_insert = ("""
    INSERT INTO songplays (
         songplay_id
        , start_time
        , user_id
        , "level"
        , song_id
        , artist_id
        , session_id
        , location
        , user_agent
    )
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent
                FROM (
                        SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                        FROM staging_events
                        WHERE page='NextSong'
                        ) events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    ;
    """)

    user_table_insert = ("""
    INSERT INTO users (
        user_id
        , first_name
        , last_name
        , gender
        , level
    )
    SELECT DISTINCT
        userId
        , firstName
        , lastName
        , gender
        , level
    FROM staging_events
    WHERE page='NextSong'
    """)

    song_table_insert = ("""
    INSERT INTO songs (
        song_id
        , title
        , artist_id
        , year
        , duration
    )
    SELECT DISTINCT
        song_id
        , title
        , artist_id
        , year
        , duration
    FROM staging_songs
    """)

    artist_table_insert = ("""
    INSERT INTO artists (
        artist_id
        , name
        , location
        , latitude
        , longitude
    )
        SELECT DISTINCT
            artist_id
            , artist_name
            , artist_location
            , artist_latitude
            , artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
    INSERT INTO time (
         start_time
        , hour
        , day
        , week
        , month
        , year
        , weekday
    )
        SELECT DISTINCT
            start_time
            , extract(hour from start_time)
            , extract(day from start_time)
            , extract(week from start_time)
            , extract(month from start_time)
            , extract(year from start_time)
            , extract(dayofweek from start_time)
        FROM songplays
    """)

    data_quality_user_id_nulls = """
    SELECT COUNT(*)
    FROM users
    WHERE user_id IS NULL
    """

    data_quality_artist_id_nulls = """
    SELECT COUNT(*)
    FROM artists
    WHERE artist_id IS NULL
    """

    data_quality_song_year = """
    SELECT COUNT(*)
    FROM songs
    WHERE year < 1500;
    """
    