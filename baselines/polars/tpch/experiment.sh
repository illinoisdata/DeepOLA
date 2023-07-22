#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 8 ]; then
    echo "Expected 8 arguments, $# given."
    echo "Usage: ./experiment.sh <data_dir> <output-dir> <scale-factor> <partition> <num-runs> <start-run> <start-query-id> <end-query-id>"
    exit
fi

data_dir=$1
output_base_dir=$2
scale=$3
partition=$4
num_runs=$5
start_run=$6
start_qdx=$7
end_qdx=$8

## Set polars parameters to disable any additional logging
export SCALE_FACTOR=$scale
export PARTITION=$partition
export INCLUDE_IO=1
export SAVE_RESULTS=0
OUTPUT_DIR=$output_base_dir/scale=$scale/partition=$partition
mkdir -p $OUTPUT_DIR

## Perform Benchmarking for Time
echo "query,run,time,peak_mem" >> $OUTPUT_DIR/timings.csv

# The experiment loop
for ((qdx = ${start_qdx}; qdx <= ${end_qdx}; qdx++)); do
    for ((j = ${start_run}; j <= ${num_runs}; j++)); do
		echo ">>> ${qdx}, run = ${j}"
		echo "Evicting Cache"
		vmtouch -e ${data_dir}/scale=${scale}/partition=${partition}/parquet
		sleep 2
		/usr/bin/time --quiet -f "$qdx,$j,%e,%M" -a -o $OUTPUT_DIR/timings.csv python -m polars_queries.q${qdx}
	done
done
