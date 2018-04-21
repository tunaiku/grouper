# Grouper Collector

A connector tool to extract data from SQL databases and import into GCP BigQuery using Apache Beam.
This tool is runnable locally, or on any other backend supported by Apache Beam, e.g. Cloud Dataflow.


**DEVELOPMENT STATUS: Alpha.**

# Overview

Grouper pipeline is a based single threaded pipeline that read all the data from single SQL database table,and converts
the data into any file and store it into appointed location, usually in GCS, Grouper pipeline runs on Apache Beam

Grouper pipeline requires the database credentials, the database table name to read, and the output location to store the
extracted data into. Tunaiku Pipeline first makes a single into the target table with limit one to infer the table schema.
After the schema is created the job ill be launched wich simply streams the table contents via JDBC into target location


## `Grouper` Java package features

   - Support PostgreSQL,Oracle and MySql JDBC connector

## `Grouper` arguments

   - `--connectionUrl`: the JDBC connection url to perform the dump
   - `--table` : the database table to query and perform the dump
   - `--output` : the path to store the data
   - `--outputTable` : the table name for destination table
   - `--username` : the database user name
   - `--password` : the database password
   - `--passwordFile` : a path to a local file containing the database password
   - `--partition` : the date of the current partition, parsed using ISODateTimeFormat.localDateOptionalTimeParser
   - `--partitionColumn` :  the name of a date/timestamp column to filter data based on current partition
   - `--partitionPeriod` : the period in which dbeam runs, used to filter based on current partition and also to check if executions are being run for a too old partition
   - `--skipPartitionCheck` : when partition column is not specified, by default fail when the partition parameter is not too old; use this avoid this behavior

## Building


Build with Gradle package to get a jar that you can run with `java -cp`. Notice this
create a fat jar. First you clean project build

```sh
mvn clean
```
build the project with Gradle pack, which will create a `grouper/target/grouper-build-[version].jar`
directory with all the dependencies, and also a shell script to run Grouper.

```sh
mvn package
```

## Examples

without partition column

```
java -jar grouper-bundled-[version].jar \
  --output=bigquery-project:database-name \
  --username=my_database_username \
  --password=secret \
  --connectionUrl=jdbc:postgresql://some.database.uri.example.org:5432/my_database \
  --table=my_table
  --runner=DirectRunner \
  --gcpTempLocation=gs://bda-storage-dev/Dataflow \ 
  --tempLocation=gs://bda-storage-dev/Dataflow/ \
  --skipPartitionCheck 
```

with partition column

```
java -jar grouper-bundled-[version].jar \
  --output=bigquery-project:database-name \
  --username=my_database_username \
  --password=secret \
  --connectionUrl=jdbc:postgresql://some.database.uri.example.org:5432/my_database \
  --table=my_table
  --runner=DirectRunner \
  --gcpTempLocation=gs://bucket-name/tmp \ 
  --tempLocation=gs://bda-storage-dev/Dataflow/ \
  --partitionColumn=created_at
  --partition="2018-02-01 23:00:00"
  --partitionPeriod="1d 0h 0min"  
```

## Requirements

Grouper build on top of apache beam,java 1.8 and gradle
make sure all of that installed on your environtment

## License

Copyright 2018 Tunaiku Amarbank.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

---
 