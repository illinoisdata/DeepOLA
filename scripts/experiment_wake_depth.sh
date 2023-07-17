#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 3 ]; then
    echo "Expected 3 arguments, $# given."
    echo "Usage: ./experiment.sh <data_dir> <num-runs> <start-run>"
    exit
fi

data_dir=$1
num_runs=$2
start_run=$3

data_params=g10_p1m_n100_c4
qdx=30
expt=accuracy
format=parquet
scale=100

# The experiment loop
for depth in {0..10}; do
    for ((j = ${start_run}; j <= ${num_runs}; j++)) do
        if [ $j -eq 0 ]; then
            expt=accuracy
        else
            expt=latency
        fi
        echo ">>> ${qdx}, depth= ${depth}, run= ${j}, expt= ${expt}"
        expt_data_dir=${data_dir}/${data_params}  # /${format}
        expt_output_dir=saved-outputs/${data_params}/depth=${depth}/run=${j}
        expt_output_dir_ref=saved-outputs/${data_params}/depth=${depth}/run=0
        mkdir -p $expt_output_dir

        echo "Evicting Cache"
        vmtouch -e ${expt_data_dir}

        query_output_dir=$expt_output_dir/q$qdx
        set -x
        RUST_LOG=warn QUERY_DEPTH=${depth} ./tpch_polars \
            query q${qdx} ${scale} \
            ${expt_data_dir} ${query_output_dir} ${expt} # 2>>$expt_output_dir/results.csv
        set +x

        if [ $j -ne 0 ]; then
            echo "Extracting accuracy at run= ${j}"
            python3 scripts/extract_accuracy.py wake ${expt_output_dir} ${expt_output_dir_ref} ${qdx}
        fi
    done
done
