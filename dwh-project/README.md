# Udacity Project: Redshift data warehouse

## Usage

Note that we will be creating our Redshift cluster using the programmatic approach rather than
manually.

- Fill out the dwh.cfg config file
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
- Create tables
  - Run `python create_tables.py`
- Run ETL to load data from S3 into Redshift
  - Run `python etl.py`
- Clean up
  - Run `python manage_cluster.py delete_cluster`

## Purpose of the database

- Sparkify's user and song database has grown, so they want to move their data to the cloud
- Song and user activity data are stored in JSON formatted log files in S3
- The goal is to create an ETL pipeline to process the S3 data into Redshift

## ETL Pipeline decisions

- The S3 data will be loaded into staging tables directly in Redshift
- The star schema tables will be populated using the staging tables
- This allows us to use SQL to easily transform data to fit the star schema
- The main thing to watch out for is duplicates
  - Examples:
    - An artist can appear multiple times in the song data
    - The same user will appear many times in the log data since it tracks individual events
    - etc
  - Since Redshift does not support DISTINCT ON, we will need to use PARTITION OVER syntax
    to exclude duplicates by primary key ID
  - In addition Redshift does not enforce primary and foreign key constraints, so we
    need to be extra careful

## Database schema design decisions

The database design uses a star schema to allow the analytics team to use simpler queries.

The combination of fact and dimension tables should allow for many types of useful queries that
the analytics team may want to answer.

There are also two staging tables which contain log data imported from S3. These tables will be used
to populate the star schema.

I have added distkeys and sortkeys to the tables for educational purposes. Since
the data we're working with is relatively small, it may have been better to keep
the distribution style as 'auto' to let Redshift decide.

### Fact Table: songplays

There is one fact table, which is the songplays table. This table contains all user events from the
logs where a user played a song. All other events were filtered out.

The songplays table will be one of the bigger tables, so we will add a sortkey and
distkey to partition and order by `start_time`. I imagine that time based queries
would be the most useful type of queries.

#### Schema

```sql
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY,
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL REFERENCES time (start_time) SORTKEY DISTKEY,
    user_id BIGINT NOT NULL REFERENCES users (user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR REFERENCES songs (song_id),
    artist_id VARCHAR REFERENCES artists (artist_id),
    session_id BIGINT NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
);
```

### Dimension Table: songs

The songs dimension can be used to ask questions, such as:

- How many users enjoyed songs from the 60's?
- How many users listened to songs less than two minutes?

#### Schema

```sql
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL REFERENCES artists (artist_id) SORTKEY DISTKEY,
    year INT NOT NULL,
    duration DOUBLE PRECISION NOT NULL
);
```
A sortkey and distkey is used to partition and sort the songs table by `artist_id` to
make joins with the artists table faster. I imagine this is a common use case.

#### Schema Notes

- There are instances in the sample data where the year is equal to 0.
  - I'm assuming this means the year is unknown

### Dimension Table: artists

The artists dimension can be used to ask some interesting questions about
location. Granted these are harder to use since not all artists have data
for location or latitude/longitude.

In addition the location column has inconsistent formatting.

Also the latitude/longitude data would likely require some GIS functionality
to be used meaningfully.

A distkey is used to partition the artists table by `artist_id` to make joins
with the songs table faster. I imagine this is a common use case.

We also sort by name since it provides some order, but also should keep the
`artist_id` contiguous unless two artists share the same name which is possible.

#### Schema

```sql
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY DISTKEY,
    name VARCHAR NOT NULL SORTKEY,
    location VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
```

#### Schema Notes

- Formatting of location column is inconsistent
  - Examples:
      - Hamilton, Ohio
      - Noci (BA)
      - New York, NY [Manhattan]
      - Fort Worth, TX
      - Denmark
      - California - SF

### Dimension Table: users

The users dimension can be used to ask useful questions about
basic demographics (gender) and also account type (free/paid).

For now we will use the 'all' distribution style since the users table is
relatively small compared to the other tables. As the user base increases
we will need to change the distribution style.

We specify a sort key of `last_name` which is an arbitrary sort order for
now. I think it is important to specify a sortkey for all tables since some
order is better than no order in most cases.

#### Schema

```sql
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL SORTKEY,
    gender CHAR(1) NOT NULL,
    level VARCHAR NOT NULL
) DISTSTYLE all;
```

### Dimension Table: time

The time dimension is one of the most interesting tables. Based on the
way the date/time parts are broken up, one can ask many fascinating questions,
such as:

- What time of day do most paid users listen to songs?
- How does the popularity of a song change from month to month?

A sortkey and distkey are added to the  `start_time` column to partition the data by time to make joins
with the songplays table faster.

#### Schema

```sql
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY SORTKEY DISTKEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
```

### Staging Table: staging_events

- This table contains log data loaded from S3.
- A sortkey has been added to `page` since we will need to filter by that column to
  populate the users, time, and songplays table
- For now, we will use 'auto' distribution, though I think it would be good to
  partition by the `ts` column. The reason I didn't was because the `ts` column
  doesn't match the format of the other keys, which means the data wouldn't be
  collocated correctly.

#### Schema

```sql
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR SORTKEY,
    registration DOUBLE PRECISION,
    sessionId BIGINT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId BIGINT
);
```

### Staging Table: staging_songs

- This table contains song data loaded from S3.
- A sortkey and distkey are added to `artist_id` to improve the performance of
  staging queries. This is because the songs and artists table are partitioned
  by `artist_id`.

#### Schema

```sql
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    duration DOUBLE PRECISION NOT NULL,
    year INT NULL,
    num_songs INT NOT NULL,
    artist_id VARCHAR NOT NULL SORTKEY DISTKEY,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR,
    artist_name VARCHAR NOT NULL
);
```
