#!/usr/bin/env bash

set -o pipefail
set -o errexit
set -o xtrace

#pull docker image
docker pull postgres

#variables
container="postgres-db" #container name
password="docker" #container password
port="5563" #port for PSQL
path="$(git rev-parse --show-toplevel)" #path to main directory
scale=1 #scale of data
partition=10 #number of partitions of data

#run container mounted onto local directory
docker run --rm   --name $container -e POSTGRES_PASSWORD=$password -d -p $port:$port -v "${path}:/deepola" postgres

sleep 10

# #change user to postgres
# docker exec $container sh -c "su postgres && exit && exit"

#create tables in PSQL
docker exec -it $container psql -U postgres -c "\i /deepola/resources/tpc-h/queries/tpch-create.sql"
docker exec -it $container psql -U postgres -c "\i /deepola/resources/tpc-h/queries/execution_stats.sql"

#copy .tbl files into database
for tbl in nation region part customer supplier partsupp orders lineitem
do
	cat "${path}"/resources/tpc-h/data/scale=$scale/partition=$partition/$tbl.tbl* | docker exec -i $container psql -U postgres -c "\copy $tbl FROM stdin WITH (FORMAT csv, DELIMITER '|')"
done

#add indexes
docker exec -it $container psql -U postgres -c "\i /deepola/resources/tpc-h/queries/tpch-alter.sql"
