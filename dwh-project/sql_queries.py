# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INT,
    lastName TEXT,
    length DOUBLE PRECISION,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration DOUBLE PRECISION,
    sessionId BIGINT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId BIGINT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id TEXT NOT NULL,
    title TEXT NOT NULL,
    duration DOUBLE PRECISION NOT NULL,
    year INT NULL,
    num_songs INT NOT NULL,
    artist_id TEXT NOT NULL,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location TEXT,
    artist_name TEXT NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY,
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL REFERENCES time (start_time),
    user_id BIGINT NOT NULL REFERENCES users (user_id),
    level TEXT NOT NULL,
    song_id TEXT REFERENCES songs (song_id),
    artist_id TEXT REFERENCES artists (artist_id),
    session_id BIGINT NOT NULL,
    location TEXT NOT NULL,
    user_agent TEXT NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender CHAR(1) NOT NULL,
    level TEXT NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL REFERENCES artists (artist_id),
    year INT NOT NULL,
    duration DOUBLE PRECISION NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from '{source_bucket}'
CREDENTIALS 'aws_iam_role={role_arn}'
REGION 'us-west-2'
FORMAT AS JSON '{json_path}';
""")

staging_songs_copy = ("""
COPY staging_songs from '{source_bucket}'
CREDENTIALS 'aws_iam_role={role_arn}'
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT into songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
(
    SELECT DISTINCT
        TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second',
        e.userId,
        e.level,
        sa.song_id,
        sa.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events e
    LEFT JOIN (
        SELECT
            s.title,
            a.name,
            s.song_id,
            s.artist_id
        FROM songs s
        JOIN artists a ON s.artist_id = a.artist_id
    ) sa ON sa.title = e.song AND sa.name = e.artist
    WHERE e.page = 'NextSong'
);
""")

user_table_insert = ("""
INSERT into users (user_id, first_name, last_name, gender, level) (
    SELECT userId, firstName, lastName, gender, level
    FROM (
        SELECT
            userId,
            firstName,
            lastName,
            gender,
            level,
            ROW_NUMBER() OVER (PARTITION BY userId) AS rank
        FROM staging_events
        WHERE page = 'NextSong'
    ) AS r
    WHERE r.rank = 1
);
""")

song_table_insert = ("""
INSERT into songs (song_id, title, artist_id, year, duration) (
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
);
""")

artist_table_insert = ("""
INSERT into artists (artist_id, name, location, latitude, longitude) (
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
);
""")

time_table_insert = ("""
INSERT into time (start_time, hour, day, week, month, year, weekday) (
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(HOUR FROM start_time),
        EXTRACT(DAY FROM start_time),
        EXTRACT(WEEK FROM start_time),
        EXTRACT(MONTH FROM start_time),
        EXTRACT(YEAR FROM start_time),
        EXTRACT(DOW FROM start_time)
    FROM staging_events
    WHERE page = 'NextSong'
    ORDER BY start_time
);
""")


# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    artist_table_create,
    song_table_create,
    user_table_create,
    time_table_create,
    songplay_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    song_table_drop,
    artist_table_drop,
    user_table_drop,
    time_table_drop,
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy,
]

insert_table_queries = [
    artist_table_insert,
    song_table_insert,
    user_table_insert,
    time_table_insert,
    songplay_table_insert,
]
