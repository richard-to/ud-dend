import configparser
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read("dl.cfg")

# Store AWS access key and secret as environment variables so we can access private S3 buckets
os.environ["AWS_ACCESS_KEY_ID"] = aws_key = config.get("AWS", "ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "SECRET_ACCESS_KEY")

# Globs are used to bulk load json data files
SONG_PATH = f"{config.get('INPUT', 'SONG_DATA')}/*/*/*/*.json"
LOG_PATH = f"{config.get('INPUT', 'LOG_DATA')}/*.json"

ANALYSIS_PATH = config.get("OUTPUT", "ANALYSIS_DATA")
DIM_SONGS_PATH = f"{ANALYSIS_PATH}/songs"
DIM_ARTISTS_PATH = f"{ANALYSIS_PATH}/artists"
DIM_USERS_PATH = f"{ANALYSIS_PATH}/users"
DIM_TIME_PATH = f"{ANALYSIS_PATH}/time"
FACT_SONGPLAYS_PATH = f"{ANALYSIS_PATH}/songplays"


def main():
    """Main ETL script for process Sparkify song/log data into analytics tables"""
    spark = SparkSession.builder.appName("Sparkify-Spark").getOrCreate()

    # Load song and events data
    songs_data_df = spark.read.json(SONG_PATH)
    log_data_df = spark.read.json(LOG_PATH)

    # Create songs dimension table
    songs_df = create_dim_songs(spark, songs_data_df)
    output_parquet(songs_df, DIM_SONGS_PATH, partition_by=["year", "artist_id"])

    # Create artists dimension table
    artists_df = create_dim_artists(spark, songs_data_df)
    output_parquet(artists_df, DIM_ARTISTS_PATH)

    # Create users dimension table
    users_df = create_dim_users(spark, log_data_df)
    output_parquet(users_df, DIM_USERS_PATH)

    # Create time dimension table
    time_df = create_dim_time(spark, log_data_df)
    output_parquet(time_df, DIM_TIME_PATH, partition_by=["year", "month"])

    # Create songplays fact table
    songplays_df = create_fact_songplays(spark, log_data_df, songs_df, artists_df)
    output_parquet(songplays_df, FACT_SONGPLAYS_PATH, partition_by=["year", "month"])


def create_dim_songs(spark, songs_data_df):
    """Creates songs dimension dateframe

    Args:
        spark: Spark client
        songs_data_df: Sparkify song data

    Returns:
        Songs dataframe
    """
    return songs_data_df.select(
         songs_data_df.song_id,
         songs_data_df.title,
         songs_data_df.artist_id,
         songs_data_df.year,
         songs_data_df.duration,
    )


def create_dim_artists(spark, songs_data_df):
    """Creates artists dimension dateframe

    Args:
        spark: Spark client
        songs_data_df: Sparkify song data

    Returns:
        Artists dataframe
    """
    return (
        songs_data_df
        # It's possible for artist_id to appear multiple times since an artist can have multiple songs
        .drop_duplicates(["artist_id"])
        .select(
            songs_data_df.artist_id,
            songs_data_df.artist_name.alias("name"),
            songs_data_df.artist_location.alias("location"),
            songs_data_df.artist_latitude.alias("latitude"),
            songs_data_df.artist_longitude.alias("longitude"),
        )
    )


def create_dim_users(spark, log_data_df):
    """Creates users dimension dateframe

    Args:
        spark: Spark client
        log_data_df: Sparkify event log data

    Returns:
        Users dataframe
    """
    return (
        log_data_df
        # It's possible for userId to appear multiple times since the user may perform multiple actions
        .drop_duplicates(["userId"])
        # Filter by NextSong since we only care about "song plays"
        .filter(log_data_df.page == "NextSong")
        .select(
            log_data_df.userId.alias("user_id"),
            log_data_df.firstName.alias("first_name"),
            log_data_df.lastName.alias("last_name"),
            log_data_df.gender,
            log_data_df.level,
        )
    )


def create_dim_time(spark, log_data_df):
    """Creates time dimension dateframe

    Args:
        spark: Spark client
        log_data_df: Sparkify event log data

    Returns:
        Time dataframe
    """
    time_df = (
        log_data_df
        # It's possible for timestamps to appear multiple times if two events happened at the same time
        .drop_duplicates(["ts"])
        # Filter by NextSong since we only care about "song plays"
        .filter(log_data_df.page == "NextSong")
        # Divide the ts column by 1000 since the timestamp is in milliseconds
        .select(F.from_unixtime(log_data_df.ts / 1000).alias("start_time"))
    )
    return time_df.select(
        time_df.start_time,
        F.hour(time_df.start_time).alias("hour"),
        F.dayofmonth(time_df.start_time).alias("day"),
        F.weekofyear(time_df.start_time).alias("week"),
        F.month(time_df.start_time).alias("month"),
        F.year(time_df.start_time).alias("year"),
        F.dayofweek(time_df.start_time).alias("weekday"),
    )


def create_fact_songplays(spark, log_data_df, songs_df, artists_df):
    """Creates songplays fact dateframe

    Args:
        spark: Spark client
        log_data_df: Sparkify event log data
        songs_df: Songs dataframe
        artists_df: Artists dateframe

    Returns:
        Songplays dataframe
    """
    # Song ID and Artist ID are needed for the songplays table. Those
    # IDs can be retrieved from the songs_df and artists_df.
    #
    # The conditions of song, artist, and duration will be to determine
    # matching records.
    songs_artists_df = (
        songs_df
        .join(artists_df, songs_df.artist_id == artists_df.artist_id)
        .select(
            songs_df.song_id,
            songs_df.artist_id,
            songs_df.title.alias("song"),
            artists_df.name.alias("artist"),
            songs_df.duration,
        )
    )

    songplays_df = (
        log_data_df
        # Filter by NextSong since we only care about "song plays"
        .filter(log_data_df.page == "NextSong")
        .select(
            # Divide the ts column by 1000 since the timestamp is in milliseconds
            F.from_unixtime(log_data_df.ts / 1000).alias("start_time"),
            log_data_df.userId.alias("user_id"),
            log_data_df.level,
            log_data_df.sessionId.alias("session_id"),
            log_data_df.location.alias("location"),
            log_data_df.userAgent.alias("user_agent"),
            log_data_df.artist,
            log_data_df.song,
            log_data_df.length.alias("duration"),
        )
    )

    songplays_df = (
        songplays_df
        .join(
            songs_artists_df,
            (songs_artists_df.song == songplays_df.song) &
            (songs_artists_df.artist == songplays_df.artist) &
            (songs_artists_df.duration == songplays_df.duration),
            # Left join is used since not all songplays will have matching Song and Artist IDs
            "left",
        )
    )

    return (
        songplays_df
        .select(
            F.monotonically_increasing_id().alias("songplays_id"),
            songplays_df.start_time,
            songplays_df.user_id,
            songplays_df.level,
            songplays_df.song_id,
            songplays_df.artist_id,
            songplays_df.session_id,
            songplays_df.location,
            songplays_df.user_agent,

            # year and month columns added for partitioning purposes
            F.year(songplays_df.start_time).alias("year"),
            F.month(songplays_df.start_time).alias("month"),
        )
    )


def output_parquet(df, output_path, partition_by=None):
    """Writes dataframe in parquet format

    Args:
        df: Dateframe to write
        output_path: Can be S3 bucket, HDFS, or local filepath
        partition_by: Optionally, columns on dateframe to partition by
    """
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(output_path)


if __name__ == "__main__":
    main()
