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

# qdx=1c
# qdx=6c
qdx=14c
expt=accuracy
# format=tbl
format=parquet

# Build benchmark script
cargo build --release --example tpch_polars

# The experiment loop
for ((j = ${start_run}; j <= ${num_runs}; j++)) do
    echo ">>> ${qdx}, run= ${j}, expt= ${expt}"
    expt_output_dir=saved-outputs/scale=${scale}/partition=${partition}/${format}/run=${j}
    expt_output_dir_ref=saved-outputs/scale=${scale}/partition=${partition}/${format}/run=0
    mkdir -p $expt_output_dir
    echo "Evicting Cache"
    # vmtouch -e ${data_dir}/scale=${scale}/partition=${partition}/${format}
    query_output_dir=$expt_output_dir/q$qdx
    set -x
    RUST_LOG=warn ./target/release/examples/tpch_polars \
        query q${qdx} ${scale} \
        ${data_dir}/scale=${scale}/partition=${partition}/${format} ${query_output_dir} ${expt} # 2>>$expt_output_dir/results.csv
    set +xh
done
