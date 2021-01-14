import configparser
import os

from pyspark.sql import SparkSession


config = configparser.ConfigParser()
config.read("dl.cfg")

# Store AWS access key and secret as environment variables so we can access private S3 buckets
os.environ["AWS_ACCESS_KEY_ID"] = aws_key = config.get("AWS", "ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "SECRET_ACCESS_KEY")

ANALYSIS_PATH = config.get("OUTPUT", "ANALYSIS_DATA")
DIM_SONGS_PATH = f"{ANALYSIS_PATH}/songs"
DIM_ARTISTS_PATH = f"{ANALYSIS_PATH}/artists"
DIM_USERS_PATH = f"{ANALYSIS_PATH}/users"
DIM_TIME_PATH = f"{ANALYSIS_PATH}/time"
FACT_SONGPLAYS_PATH = f"{ANALYSIS_PATH}/songplays"


def main():
    """Checks that the Sparkify fact and dimension tables were created correctly

    For each table:

    - Show the first 20 rows
    - Print the count of rows
    """
    spark = SparkSession.builder.appName("Sparkify-Spark-Check").getOrCreate()

    data_paths = [
        DIM_SONGS_PATH,
        DIM_ARTISTS_PATH,
        DIM_USERS_PATH,
        DIM_TIME_PATH,
        FACT_SONGPLAYS_PATH,
    ]
    for path in data_paths:
        df = spark.read.parquet(path)
        df.show()

        print(f"{path} has {df.count()} total row(s).")
        print()


if __name__ == "__main__":
    main()
