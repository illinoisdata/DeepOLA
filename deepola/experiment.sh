#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 5 ]; then
	echo "Expected 5 arguments, $# given."
	echo "Usage: ./experiment.sh <data_dir> <scale-factor> <partition> <num-runs> <start-run>"
	exit
fi

data_dir=$1
scale=$2
partition=$3
num_runs=$4
start_run=$5
format=parquet

# Build benchmark script
cargo build --release --example tpch_polars

# The experiment loop
for qdx in {1..22}
do
    for ((j = ${start_run}; j <= ${num_runs}; j++))
    do
	if [ $j -eq 0 ]; then
		expt=accuracy
	else
		expt=latency
	fi
	echo ">>> ${qdx}, run= {$j}, expt= ${expt}"
	expt_output_dir=saved-outputs/scale=${scale}/partition=${partition}/${format}/run=${j}
	mkdir -p $expt_output_dir
	echo "Evicting Cache"
	vmtouch -e ${data_dir}/scale=${scale}/partition=${partition}/${format}
        query_output_dir=$expt_output_dir/q$qdx
        set -x
        RUST_LOG=warn /usr/bin/time -f "$qdx,$j,$expt,%e,%M" ./target/release/examples/tpch_polars \
            query q${qdx} ${scale} \
            ${data_dir}/scale=${scale}/partition=${partition}/${format} ${query_output_dir} ${expt} 2>>$expt_output_dir/results.csv
        set +x
    done
done
