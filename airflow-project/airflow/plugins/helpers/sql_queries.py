class SqlQueries:
    """SQL Queries used during the LoadFactOperator and LoadDimensionOperator steps"""

    songplay_table_insert = ("""
    SELECT
        md5(events.sessionId || events.start_time) songplayid,
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
        WHERE page = 'NextSong'
    ) events
    LEFT JOIN staging_songs songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
    """)

    user_table_insert = ("""
    SELECT userid, firstname, lastname, gender, level
    FROM (
        SELECT
            userid,
            firstname,
            lastname,
            gender,
            level,
            ROW_NUMBER() OVER (PARTITION BY userid) AS rank
        FROM staging_events
        WHERE page = 'NextSong'
    ) AS r
    WHERE r.rank = 1
    """)

    song_table_insert = ("""
    SELECT song_id, title, artist_id, year, duration
    FROM (
        SELECT
            song_id,
            title,
            artist_id,
            year,
            duration,
            ROW_NUMBER() OVER (PARTITION BY song_id) AS rank
        FROM staging_songs
    ) AS r
    WHERE r.rank = 1
    """)

    artist_table_insert = ("""
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM (
        SELECT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude,
            ROW_NUMBER() OVER (PARTITION BY artist_id) AS rank
        FROM staging_songs
    ) AS r
    WHERE r.rank = 1
    """)

    time_table_insert = ("""
    SELECT DISTINCT
        start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(dayofweek from start_time)
    FROM songplays
    """)
