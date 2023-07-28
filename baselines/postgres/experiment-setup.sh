#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 5 ]; then
    echo "Expected 5 arguments, $# given."
    echo "Usage: ./experiment-setup.sh <data_dir> <query_dir> <postgres_dir> <scale-factor> <partition>"
    exit
fi

data_dir=$1
query_dir=$2
postgres_dir=$3
scale=$4
partition=$5

## Postgres setup
echo ">>> Setting up Postgres Container"
docker container stop pgsql
docker run --rm --name pgsql -e POSTGRES_PASSWORD=docker -d -v $postgres_dir:/var/lib/postgresql/data:rw -v $query_dir:/dataset/tpch/queries:rw postgres

## Wait for docker container and postgres server to come up
sleep 5

docker exec -it pgsql psql -U postgres -c "\i /dataset/tpch/queries/tpch-create.sql"
docker exec -it pgsql psql -U postgres -c "\i /dataset/tpch/queries/execution_stats.sql"

#copy .tbl files into database
for tbl in nation region part customer supplier partsupp orders lineitem
do
	cat $data_dir/scale=$scale/partition=$partition/tbl/$tbl.tbl* | docker exec -i pgsql psql -U postgres -c "\copy $tbl FROM stdin WITH (FORMAT csv, DELIMITER '|')"
done

docker exec -it pgsql psql -U postgres -c "\i /dataset/tpch/queries/tpch-alter.sql"
