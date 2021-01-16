# Udacity Project: Airflow Data Pipeline

## Usage instructions

### 1. Create Redshift cluster

- Fill out the missing settings in the `dwh.cfg` config file
  - The AWS KEY and SECRET should be for a AWS user that has programmatic access and
    the AdministratorAccess privilege
- Create the Redshift cluster
  - Run `python manage_cluster.py create_cluster`
- Wait until the cluster status becomes "available"
  - Use `python manage_cluster.py check_status` to check the status of the cluster
- Enable VPC access to the cluster
  - Run `python manage_cluster.py enable_vpc_access`
  - For educational purposes, this will add an ingress rule that will allow open access to the cluster
    - In a production environment, you'd want to manage access more granularly
  - The ingress rule will be added to the security group named "default"
    - This is the default security group when creating a Redshift cluster

### 2. Create tables on Redshift cluster

- Run `python create_tables.py`
  - This command will drop the existing tables and recreate the the tables

### 3. Start Airflow

- In the Udacity project workspace run: `/opt/airflow/start.sh`

### 4. Add AWS Credentials connection in Airflow

- Conn ID = aws_credentials
- Conn Type = Amazon Web Services
- Login = AWS Key
- Password = AWS Secret Key

### 5. Add Redshift connection in Airflow

- Conn ID = redshift
- Conn Type = Postgres
- Host = Redshift cluster host name
  - This value can be retrieved from the AWS Redshift console
  - Or you can run `python manage_cluster.py check_status`
- Schema = `DB_NAME` from `dwh.cfg`
- Login = `DB_USER` from `dwh.cfg`
- Password = `DB_PASSWORD` from `dwh.cfg`
- Port = `DB_PORT` from `dwh.cfg`

### 6. Turn on DAG

- To run the DAG, turn on the `sparkify_etl_dag` DAG
