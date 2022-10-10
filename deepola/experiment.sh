#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 4 ]; then
	echo "Expected 4 arguments, $# given."
	echo "Usage: ./experiment.sh <data_dir> <scale-factor> <partition> <num-runs>"
	exit
fi

data_dir=$1
scale=$2
partition=$3
num_runs=$4
format=parquet

# Build benchmark script
cargo build --release --example tpch_polars

# The experiment loop
for qdx in {1..22}
do
    for ((j = 0; j < ${num_runs}; j++))
    do
        echo ">>> ${qdx}, run= {$j}"
	expt_output_dir=saved-outputs/scale=${scale}/partition=${partition}/${format}/run=${j}
	mkdir -p $expt_output_dir
	echo "Evicting Cache"
	vmtouch -e ${data_dir}/scale=${scale}/partition=${partition}/${format}
        query_output_dir=$expt_output_dir/q$qdx
        set -x
        RUST_LOG=warn /usr/bin/time -f "$qdx,$j,%e,%M" ./target/release/examples/tpch_polars \
            query q${qdx} ${scale} \
            ${data_dir}/scale=${scale}/partition=${partition}/${format} ${query_output_dir} 2>>$expt_output_dir/results.csv
        set +x
    done
done
