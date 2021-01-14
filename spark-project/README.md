# Udacity Project: Spark data lake

## 1. Usage

### 1.1 Analysis data S3 bucket

- Pick or create an S3 bucket that will be used for storing the output of the analysis tables
- This bucket path will be specified in `dl.cfg`

### 1.2 Config file

In order to run `etl.py`, the `dl.cfg` file must be configured:

- **AWS section**
  - An AWS access key and secret will be needed for reading/writing output to your private S3 bucket
- **Input section**
  - LOG_DATA and SONG_DATA can point to an S3 bucket, HDFS, or a local file path
  - For this project, we'll want to use the S3 buckets provided by Udacity, which have public access
- **Output section**
  - ANALYSIS_DATA can point to an S3 bucket, HDFS, or a local file path
  - For this project, we'll want to use an S3 bucket (see step 1.1)

### 1.3 Create a Key Pair

In order to connect to your cluster, we'll need an EC2 key pair. You can create one via the [AWS console](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#KeyPairs:).

You'll need to specify the key in the next step.

### 1.4 Create EMR cluster

Next create a cluster. You can do this manually via the [AWS console](https://us-west-2.console.aws.amazon.com/elasticmapreduce/home?region=us-west-2#cluster-list).

Or you can use the AWS CLI (note: replace `<YOUR-KEY-NAME>` with your key):

```bash
aws emr create-default-roles

aws emr create-cluster --name spark-data-lake --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark  --ec2-attributes KeyName=<YOUR-KEY-NAME> --instance-type m5.xlarge --instance-count 3
```

If using the CLI, you can check when your cluster is ready (i.e. has a status of "WAITING") by running this command. You need to
specify the cluster ID which you can retrieve from the output of the `create-cluster` command.

```bash
aws emr describe-cluster --cluster-id <CLUSTER-ID>
```

This previous command will also return a public domain name, which you can use to SSH into your cluster. Look for a line called
`MasterPublicDnsName`. It should have a value that looks like this `ec2-100-20-156-158.us-west-2.compute.amazonaws.com`.

### 1.5 Connect to EMR cluster

- To connect to your cluster, you can use SSH. On Linux/MacOS, you'll need to download your key pair as a PEM file.
- The public domain of your master node can be retrieved from the previous section or the AWS EMR console.
- One last thing you'll need to do in order to connect to your cluster is allow SSH access from your local machine to
  your cluster.
  - This is done by updating the ingress rules of the Security Group for your cluster's master node. You'll want to add
    an inbound rule allowing SSH from the IP of your local machine.

```bash
ssh -i <PEM-KEY-PAIR> hadoop@<CLUSTER-HOST>
```

### 1.6 Use Python3 on EMR cluster

If you created your cluster through the CLI, you'll want to ensure that `spark-submit` runs using Python3 (and not Python2)
since `etl.py` uses Python3 specific syntax. You can do this by updating the `PYSPARK_PYTHON` environment variable.

```bash
export PYSPARK_PYTHON=python3
```

### 1.7 Upload Spark scripts and config

You can use scp to transfer the following files to your cluster.

- dl.cfg
- etl.py

### 1.8 Running etl.py

You should now be able to run `etl.py` on the cluster:

```bash
spark-submit etl.py
```

### 1.9 Delete EMR cluster

```bash
aws emr terminate-clusters --cluster-ids <CLUSTER-ID>
```

## 2. Purpose of the database

Sparkify has grown their user and song database further. They want to move their data warehouse data to
a data lake.

Our goal is to create the same star schema used in the data warehouse, but instead we'll store the data
in parquet format in S3. Then the Sparkify analytics team can use Spark to analyze the data.

## 3. Schema design decisions

The data lake design uses the same star schema used in the Redshift data warehouse. We kept the design
the same since the analytics team is already familiar with the schema. Spark has a SQL syntax which
should allow them to use almost the same queries. The nice thing about this is that the analytics team
doesn't need to know about the underlying data storage.
