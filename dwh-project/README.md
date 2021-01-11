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

### Fact Table: songplays

There is one fact table, which is the songplays table. This table contains all user events from the
logs where a user played a song. All other events were filtered out.

#### Schema

```sql
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
```

### Dimension Table: songs

The songs dimension can be used to ask questions, such as:

- How many users enjoyed songs from the 60's?
- How many users listened to songs less than two minutes?

#### Schema

```sql
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL REFERENCES artists (artist_id),
    year INT NOT NULL,
    duration DOUBLE PRECISION NOT NULL
);
```

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

#### Schema

```sql
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
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

#### Schema

```sql
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender CHAR(1) NOT NULL,
    level TEXT NOT NULL
);
```

### Dimension Table: time

The time dimension is one of the most interesting tables. Based on the
way the date/time parts are broken up, one can ask many fascinating questions,
such as:

- What time of day do most paid users listen to songs?
- How does the popularity of a song change from month to month?

#### Schema

```sql
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
```

### Staging Table: staging_events

This table contains log data loaded from S3.

#### Schema

```sql
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
```

### Staging Table: staging_songs

This table contains song data loaded from S3.

#### Schema

```sql
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
```
