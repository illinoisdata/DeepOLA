#!/usr/bin/env bash

set -o pipefail
set -o errexit
set -o xtrace

#pull sudo docker image
sudo docker pull postgres

#variables
container="pgsql" #container name
password="sudo docker" #container password
port="5563" #port for PSQL
path="$(git rev-parse --show-toplevel)" #path to main directory
scale=50 #scale of data
partition=1 #number of partitions of data

#run container mounted onto local directory
#sudo docker run --rm  --name $container -e POSTGRES_PASSWORD=$password -d -p $port:$port -v "${path}:/deepola" postgres

sleep 10

# #change user to postgres
# sudo docker exec $container sh -c "su postgres && exit && exit"

#create tables in PSQL
sudo docker exec -it $container psql -U postgres -c "\i /home/deepola/DeepOLA/resources/tpc-h/queries/tpch-create.sql"
sudo docker exec -it $container psql -U postgres -c "\i /home/deepola/DeepOLA/resources/tpc-h/queries/execution_stats.sql"

#copy .tbl files into database
for tbl in nation region part customer supplier partsupp orders lineitem
do
	cat /mnt/DeepOLA/resources/tpc-h/data/scale=$scale/partition=$partition/tbl/$tbl.tbl* | sudo docker exec -i $container psql -U postgres -c "\copy $tbl FROM stdin WITH (FORMAT csv, DELIMITER '|')"
done

#add indexes
sudo docker exec -it $container psql -U postgres -c "\i /home/deepola/DeepOLA/resources/tpc-h/queries/tpch-alter.sql"
