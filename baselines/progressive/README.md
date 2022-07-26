# Tutorial on ProgressiveDB


## Preparation

### Download

You can download ProgressiveDB with it's relative files by running the following command: `git clone git@github.com:illinoisdata/DeepOLA.git`.

### Navigation

You can find all ProgressiveDB files in the following path: `DeepOLA/java`.


### Sanity Check

You should have following in the `java` dir:

1. README.md
2. progressive-db.conf
3. progressive-db.jar
4. Client
5. Server
6. DataClean


## Setup

### File

You need to modify the file `progressive-db.conf` to customize your configuration.

### Default

`source.url=jdbc:postgresql://localhost:5432/progressive`

`source.user=...`

`source.password=...`

`meta.url=jdbc:sqlite:progressivedb.sqlite`

`tmp.url=jdbc:sqlite::memory:`

`chunkSize=100000`

### Explanation

#### Required

1. `source.url=jdbc:postgresql://localhost:5432/progressive`

This line sets up where your data will be stored.

The default setting is using PostgresSQL, which explains the part `jdbc:postgresql://localhost:5432`. If you want to use any other driver such as MySQL, you should substitute the name here and change the port number accordingly.

`progressive` indicates in which database will you store and manipulate your data. You can choose your own here but don't forget to create it in your own driver (PostgreSQL in the default setting).

2. `meta.url=jdbc:sqlite:progressivedb.sqlite` & `tmp.url=jdbc:sqlite::memory:`

Those two lines set up where your meta data and caching results will be stored.

You should keep them without making any change (default is good).

#### Optional

1. `source.user={username}` & `source.password={password}`

Those two lines are required if authentication is required for using PostgreSQL (or any other driver of your choice).

2. `chunkSize=100000`

This line of configuration is optional but hightly recommended.

It decides the speed and duration of the partitioning process.

If your data size is small, you set a small value for your chunkSize, such as 2. If your data size is big, you should pick a large number such as 10000 instead.

Improper chunkSize may result issues like missing partitions or never-ending partitioning process.


## Run

### Server

#### Location

Make sure you are located in the `java` dir.

#### Command

You can use the following command to turn on the Server: `java -jar progressive-db.jar`.


### Data

#### Login

You should login to your PostgreSQL (or other driver of your choice) first. If you are using the default PostgreSQL, you should use the command: `psql postgres -U {username}`.

#### Choose database

You can use the command `\l` to list all available databases.

You can use the command `\c` to select a specific database.

The database you select here should match the database you put in the file `progressive-db.conf` in the setup step.

#### Work with tables

You can use the command `\dt` to list all available all tables.

#### Create table

You can create a table here using the command `Create Table {name}();` or write a Main file in Client (explained later). It is recommended to create the table here directly which is a little bit easier.

#### Load data

You can bulk load data into the table you just created using the command `\copy {table name} from '/path/to/the/data/data.csv' delimiter ',' csv header`;

You can also wirte a Main file in Client (same as above) to load data. However, you should only do so if there is a limit amount of data and and using `\copy` command is always highly recommended.

### Client

#### Location

Make sure you are located in the `Client` dir.

#### Command

You can use the following command to run your Client: `mvn compile exec:java -Dexec.mainClass="path.to.your.client.Main"`

### Notice

You should run your Server and Client in two separate windows.

Futher explanation on how to run and edit your Client files will be lised below.


## Client

### Preparation

Your Client dir should have file `pom.xml` and dir `src`.

You should run `mvn compile` command in this level, but if you want to edit your Client or make new ones, you should go further in the `src` dir.

### Breakdown

If you follow the path: `DeepOLA/java/Client/src/main/java/edu/illinois/deepola/suwen`, you will then be able to find two dir:

`load` and `query`.

`load` store files to load the data into your database as well as partitioning your tables.

`query` store files to query your data and generating corresponding results.

### Compatibility

ProgressiveDB currently supports querying on a single table only.

Due to this reason, only TPCH query #1 and #6 are supported, both relying on the table `lineitem`. We have the file for loading this table, as well as the table `skewed`, which is a skewed version of `lineitem` for the purposing of comparing between ProgressiveDB and some other database systems (if needed).

### load

There are two dir in `load`: `lineitem` and `skewed`, both have a Main file inside to store the data and partition the table.

`statement.execute("PREPARE TABLE skewed");` This line of code is used to partition the table (the table named skewed in this case).

Code commented out in those two Main files should be used to create a table and load data into the table. Since it is recommended to bulk insert your data using the `\copy` command, they are commented out. Where they are still left here because they can be a good reference to show how creating a table and inserting data works.

### query

There are code in this dir to run TPCH query 1 and 6, for both table `lineitem` and `skewed`.

The pacakge name at the beginning of each Main file is used as the address to put into the command: `mvn compile exec:java -Dexec.mainClass="package.name.Main"`. (It is the same with running Main files from load)



# Tutorial on Data Cleaning

## Preparation

You should have a dir call `DataClean` and all following are done in this dir.

## Convertor

It has a sample file that converts a tpch table file (`.tbl`) that you can download online to a `csv` file that you can use to insert data into your database and modify it by yourself.

## Latency

This dir contains files that can help you calculate the latency of the query. You should plug in the file in which holds your data for time and run the command `mvn compile exec:java -Dexec.mainClass=progressive.Main"` to generate the result.

## Accuracy

This dir contains files that can help you calcualte the accuracy of the query. You should plug in the file in which holds your data for output and run the command `mvn compile exec:java -Dexec.mainClass=progressive.Main"` to generate the result.

## Merge

This dir is only in need if there are multiply measurement / groups for your query. If you want to merge multiply measurements, you should go with the `col` dir to merge all values (collapse the col). If you want to merge multiply groups, you should go with the `row` dir to merge all values (collase the row).


# End

## Save

You can add `> {filename}` at the end of the `mvn compile` command after querying the data, which will help you to save the result into a separate file.

## Java files

There are multiply java files included in the project, and `mvn compile` different java files may yield different results.

Since ProgressiveDB is currently supporting queries that involving one table, only TPCH query 1 & 6 are supported at this stage, both using table lineitem.

If you want to use the regular lineitem table, choose files in `lineitem` direcotory. If you want to use lineitem with skewed data, you should go to dir `skewed`. For each dir, there are separate files for query 1 & 6.


