#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 9 ]; then
    echo "Expected 9 arguments, $# given."
    echo "Usage: ./experiment.sh <data_dir> <query_dir> <output_dir> <scale-factor> <partition> <num-runs> <start-run> <start-query-id> <end-query-id>"
    exit
fi

data_dir=$1
query_dir=$2
output_dir=$3
scale=$4
partition=$5
num_runs=$6
start_run=$7
start_qdx=$8
end_qdx=$9

file_path="${data_dir}/scale=${scale}/partition=${partition}/tbl/"
DB="wander_${scale}_${partition}"

## Start Postgres
postgres -D /usr/local/pgsql/data &
sleep 5

bash experiment-setup.sh $data_dir $scale $partition
bash experiment-time.sh $query_dir $output_dir $scale $partition $num_runs $start_run $start_qdx $end_qdx
