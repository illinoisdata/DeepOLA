#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 9 ]; then
    echo "Expected 9 arguments, $# given."
    echo "Usage: ./experiment_postgres.sh <query_dir> <output_dir> <postgres_dir> <scale-factor> <partition> <num-runs> <start-run> <start-query-id> <end-query-id>"
    exit
fi

query_dir=$1
output_dir=$2
postgres_dir=$3
scale=$4
partition=$5
num_runs=$6
start_run=$7
start_qdx=$8
end_qdx=$9

## Postgres setup
echo ">>> Run the Postgres Container"
docker container stop pgsql
docker run --rm --name pgsql -e POSTGRES_PASSWORD=docker -d -v $postgres_dir:/var/lib/postgresql/data:rw -v $query_dir:/dataset/tpch/queries:rw postgres

## Wait for docker container and postgres server to come up
sleep 5

mkdir -p $output_dir

## The experiment loop
for ((qdx = ${start_qdx}; qdx <= ${end_qdx}; qdx++)); do
    for ((j = ${start_run}; j <= ${num_runs}; j++)); do
        echo ">>> ${qdx}, run= ${j}"

        echo "Evicting Cache"
		docker exec -it pgsql psql -U postgres -c "DISCARD ALL;"

		docker exec -it pgsql psql -U postgres -c "\pset pager off" -c "\timing" -f "/dataset/tpch/queries/$qdx.sql" > $output_dir/q$qdx-run$j.log
    done
done

## Stop the docker container
docker container stop pgsql
