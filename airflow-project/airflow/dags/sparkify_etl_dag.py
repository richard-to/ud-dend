from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,

)

from helpers import DataCheckFactory, SqlQueries


default_args = {
    "depends_on_past": False,
    "email_on_retry": False,
    "owner": "udacity",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2020, 1, 14),
}

dag = DAG(
    "sparkify_etl_dag",
    catchup=False,
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)


start_operator = DummyOperator(task_id="begin_execution", dag=dag)

# Stage S3 data into Redshift
# ---------------------------

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket_path="s3://udacity-dend/log_data",
    target_table="staging_events",
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songs",
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket_path="s3://udacity-dend/song_data",
    target_table="staging_songs",
)


# Load fact table
# ---------------

load_songplays_table = LoadFactOperator(
    task_id="load_songplays_fact_table",
    dag=dag,
    columns=[
        "songplayid",
        "start_time",
        "userid",
        "level",
        "songid",
        "artistid",
        "sessionid",
        "location",
        "user_agent",
    ],
    redshift_conn_id="redshift",
    select_sql=SqlQueries.songplay_table_insert,
    target_table="songplays",
)


# Load dimension tables
# ---------------------

load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dim_table",
    dag=dag,
    columns=["userid", "first_name", "last_name", "gender", "level"],
    redshift_conn_id="redshift",
    select_sql=SqlQueries.user_table_insert,
    target_table="users",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="load_song_dim_table",
    dag=dag,
    columns=["songid", "title", "artistid", "year", "duration"],
    redshift_conn_id="redshift",
    select_sql=SqlQueries.song_table_insert,
    target_table="songs",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dim_table",
    dag=dag,
    columns=["artistid", "name", "location", "latitude", "longitude"],
    redshift_conn_id="redshift",
    select_sql=SqlQueries.artist_table_insert,
    target_table="artists",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    dag=dag,
    columns=["start_time", "hour", "day", "week", "month", "year", "weekday"],
    redshift_conn_id="redshift",
    select_sql=SqlQueries.time_table_insert,
    target_table="time",
)


# Data quality checks
# -------------------

run_user_quality_checks = DataQualityOperator(
    task_id="qa_users_table",
    dag=dag,
    redshift_conn_id="redshift",
    params={
        "data_checks": [
            DataCheckFactory.create_no_data_check("users", "userid"),
            DataCheckFactory.create_duplicates_check("users", "userid"),
            DataCheckFactory.create_valid_values_check("users", "gender", ["M", "F"]),
            DataCheckFactory.create_valid_values_check("users", "level", ["free", "paid"]),
        ],
    },
)

run_song_quality_checks = DataQualityOperator(
    task_id="qa_songs_table",
    dag=dag,
    redshift_conn_id="redshift",
    params={
        "data_checks": [
            DataCheckFactory.create_no_data_check("songs", "songid"),
            DataCheckFactory.create_duplicates_check("songs", "songid"),
        ],
    },
)

run_artist_quality_checks = DataQualityOperator(
    task_id="qa_artists_table",
    dag=dag,
    redshift_conn_id="redshift",
    params={
        "data_checks": [
            DataCheckFactory.create_no_data_check("artists", "artistid"),
            DataCheckFactory.create_duplicates_check("artists", "artistid"),
        ],
    },
)

run_time_quality_checks = DataQualityOperator(
    task_id="qa_time_table",
    dag=dag,
    redshift_conn_id="redshift",
    params={
        "data_checks": [
            DataCheckFactory.create_no_data_check("time", "start_time"),
            DataCheckFactory.create_duplicates_check("time", "start_time"),
        ],
    },
)

end_operator = DummyOperator(task_id="stop_execution", dag=dag)


# Build DAG
# ---------

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table]

load_user_dimension_table >> run_user_quality_checks
load_song_dimension_table >> run_song_quality_checks
load_artist_dimension_table >> run_artist_quality_checks
load_time_dimension_table >> run_time_quality_checks

[run_user_quality_checks, run_song_quality_checks,
 run_artist_quality_checks, run_time_quality_checks] >> end_operator
