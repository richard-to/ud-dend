import configparser

import boto3
import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def main():
    """Entrypoint to drop/create Redshift tables"""

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

    # Connect to Redshift cluster
    conn = psycopg2.connect(
        f"host={db_host} dbname={db_name} user={db_user} password={db_password} port={db_port}"
    )
    cur = conn.cursor()

    # Drop tables before recreating them to ensure a clean environment
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


if __name__ == "__main__":
    main()
