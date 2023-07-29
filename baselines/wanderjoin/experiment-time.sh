#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 8 ]; then
    echo "Expected 8 arguments, $# given."
    echo "Usage: ./experiment-time.sh <query_dir> <output_dir> <scale-factor> <partition> <num-runs> <start-run> <start-query-id> <end-query-id>"
    exit
fi

query_dir=$1
output_dir=$2
scale=$3
partition=$4
num_runs=$5
start_run=$6
start_qdx=$7
end_qdx=$8

DB="wander_${scale}_${partition}"

## The experiment loop
for ((qdx = ${start_qdx}; qdx <= ${end_qdx}; qdx++)); do
    for ((j = ${start_run}; j <= ${num_runs}; j++)); do
        echo ">>> ${qdx}, run= ${j}"
        echo "Evicting Cache"
		psql -d ${DB} -c "DISCARD ALL;"
        psql -d ${DB} -f /wanderjoin/queries/${qdx}.sql -o ${output_dir}/${qdx}.csv -F ',' -A
    done
done

## Stop the docker container
docker container stop pgsql
