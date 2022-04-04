#!/bin/bash
#setup_postgres.sh
#PostgreSQL version 14.2

#pull docker image
docker pull postgres

#variables
container="postgres-db" #container name
password="docker" #container password
port="5563" #port for PSQL
path="$HOME/DeepOLA" #path to main directory
scale=1 #scale of data
partition=1 #number of partitions of data

#run container mounted onto local directory
docker run --rm   --name $container -e POSTGRES_PASSWORD=$password -d -p $port:$port -v $path/baselines/postgres/results:/deepola postgres

#change user to postgres
docker exec $container sh -c "su postgres && exit && exit"

#create tables in PSQL
docker exec -it $container psql -U postgres -c "\i /deepola/baselines/resources/tpch-create.sql"
docker exec -it $container psql -U postgres -c "\i /deepola/baselines/resources/execution_stats.sql"

#copy .tbl files into database
for tbl in nation region part customer supplier partsupp orders lineitem
do
	docker exec -it $container psql -U postgres -c "\copy $tbl FROM '/deepola/data/tpc-h/scale=$scale/partition=$partition/$tbl.tbl' WITH (FORMAT csv, DELIMITER '|')"
done

#add indexes
docker exec -it $container psql -U postgres -c "\i /deepola/baselines/resources/tpch-alter.sql"

#run the queries
for i in {1..22}
do
	docker exec -it $container psql -U postgres -c "\pset pager off" -c "\timing" -c "\i /deepola/baselines/resources/$i.sql" -c "\timing" > $path/baselines/postgres/results/result_$i.csv
done

#kill container
docker kill $container
