import configparser

import boto3
import psycopg2

from sql_queries import staging_events_copy, staging_songs_copy, insert_table_queries


def main():
    """Entrypoint into ETL script which will load S3 data into Redshift"""

    # Load config
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    aws_key = config.get("AWS", "KEY")
    aws_secret = config.get("AWS", "SECRET")

    db_cluster_id = config.get("DWH", "CLUSTER_IDENTIFIER")
    db_name = config.get("DWH", "DB_NAME")
    db_user = config.get("DWH", "DB_USER")
    db_password = config.get("DWH", "DB_PASSWORD")
    db_port = config.get("DWH", "DB_PORT")

    s3_log_data = config.get("S3", "LOG_DATA")
    s3_log_json_path = config.get("S3", "LOG_JSONPATH")
    s3_song_data = config.get("S3", "SONG_DATA")

    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
    )

    # Make sure the Redshift cluster exists
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=db_cluster_id)["Clusters"][0]
    except redshift.exceptions.ClusterNotFoundFault:
        print("Error: Cluster does not exist.")
        return

    if cluster_props["ClusterStatus"] != "available":
        print(f"Error: Cluster is not available. Current status is: {cluster_props['ClusterStatus']}")
        return

    # Dynamically retrieve the Redshift cluster host
    db_host = cluster_props["Endpoint"]["Address"]

    # Dynamically retrieve Role ARN so we can access S3 buckets
    role_arn = cluster_props["IamRoles"][0]["IamRoleArn"]

    # Connect to Redshift cluster
    conn = psycopg2.connect(
        f"host={db_host} dbname={db_name} user={db_user} password={db_password} port={db_port}"
    )
    cur = conn.cursor()

    # Load staging data from S3
    load_staging_events(cur, conn, s3_log_data, s3_log_json_path, role_arn)
    load_staging_songs(cur, conn, s3_song_data, role_arn)

    # Populate star schema using loaded staging data
    insert_tables(cur, conn)

    conn.close()


def load_staging_events(cur, conn, s3_log_data, s3_log_json_path, role_arn):
    """Inserts log data from S3 into staging_events table"""
    cur.execute(staging_events_copy.format(
        source_bucket=s3_log_data,
        role_arn=role_arn,
        json_path=s3_log_json_path,
    ))
    conn.commit()


def load_staging_songs(cur, conn, s3_song_data, role_arn):
    """Inserts song data from S3 into staging_songs table"""
    cur.execute(staging_songs_copy.format(
        source_bucket=s3_song_data,
        role_arn=role_arn,
    ))
    conn.commit()


def insert_tables(cur, conn):
    """Inserts data into star schema tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


if __name__ == "__main__":
    main()
