#!/usr/bin/env bash
if [ "$#" -ne 2 ]; then
	echo "Usage: ./1-setup-postgres.sh <scale> <partition>"
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
container="pgsql-$scale" #container name
password="docker" #container password
port="5563" #port for PSQL
path="$(git rev-parse --show-toplevel)" #path to main directory

#directory for postgres data
mkdir -p /mnt/postgres-$scale

#run container mounted onto local directory
sudo docker run --name $container -e POSTGRES_PASSWORD=$password -d -v "$path:$path" -v "/mnt/postgres-$scale:/var/lib/postgresql/data" -v "/mnt/DeepOLA:/mnt/DeepOLA" postgres

sleep 10

#create tables in PSQL
sudo docker exec -it $container psql -U postgres -c "\i $path/resources/tpc-h/queries/tpch-create.sql"
sudo docker exec -it $container psql -U postgres -c "\i $path/resources/tpc-h/queries/execution_stats.sql"

#copy .tbl files into database
for tbl in nation region part customer supplier partsupp orders lineitem
do
	cat /mnt/DeepOLA/resources/tpc-h/data/scale=$scale/partition=$partition/tbl/$tbl.tbl* | sudo docker exec -i $container psql -U postgres -c "\copy $tbl FROM stdin WITH (FORMAT csv, DELIMITER '|')"
done

#add indexes
sudo docker exec -it $container psql -U postgres -c "\i $path/resources/tpc-h/queries/tpch-alter.sql"
