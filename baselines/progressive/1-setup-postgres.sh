#!/usr/bin/env bash
if [ "$#" -ne 3 ]; then
	echo "Usage: ./1-setup-postgres.sh <scale> <partition> <variation>"
	exit
fi

set -o pipefail
set -o errexit
set -o xtrace

#pull sudo docker image
sudo docker pull postgres

#variables
scale=$1 #scale of data
partition=$2 #number of partitions of data
variation=$3
container="pgsql-$scale-$variation" #container name
password="docker" #container password
port="5563" #port for PSQL
path="$(git rev-parse --show-toplevel)" #path to main directory
database=progressive

#directory for postgres data
mkdir -p /mnt/$container

if [ ! "$(sudo docker ps -q -f name=$container)" ]; then
	#run container mounted onto local directory
	sudo docker run --name $container -e POSTGRES_PASSWORD=$password -d -v "$path:$path" -v "/mnt/$container:/var/lib/postgresql/data" -v "/mnt/DeepOLA:/mnt/DeepOLA" -p 5432:5432 postgres
	sleep 10
fi

#create database
sudo docker exec -it $container psql -U postgres -c "CREATE DATABASE $database"

#create tables in PSQL
sudo docker exec -it $container psql -U postgres -d $database -c "\i $PWD/sql-queries/create-table.sql"

#copy .tbl files into database
INPUT_DIR=/mnt/DeepOLA/resources/tpc-h/$variation/scale=$scale/partition=$partition/cleaned-tbl
echo $INPUT_FILES
for file in $INPUT_DIR/lineitem.tbl.*
do
	echo $file
	## \copy
	cat $file | sudo docker exec -i $container psql -U postgres -d $database -c "\copy lineitem FROM stdin WITH (FORMAT csv, DELIMITER '|')"

	## COPY
	#sudo docker exec -i $container psql -U postgres -c "COPY lineitem FROM '$file' WITH (FORMAT csv, DELIMITER ',')"
done

#add indexes
sudo docker exec -it $container psql -U postgres -d $database -c "\i $PWD/sql-queries/create-index.sql"
