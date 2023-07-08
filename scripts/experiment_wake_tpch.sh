#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 7 ]; then
    echo "Expected 7 arguments, $# given."
    echo "Usage: ./experiment_wake_tpch.sh <data_dir> <scale-factor> <partition> <num-runs> <start-run> <start-query-id> <end-query-id>"
    exit
fi

data_dir=$1
scale=$2
partition=$3
num_runs=$4
start_run=$5
start_qdx=$6
end_qdx=$7

# The experiment loop
for ((qdx = ${start_qdx}; j <= ${end_qdx}; qdx++)); do
    if [ ${qdx} -eq 26 ] || [ ${qdx} -eq 27 ]; then
        format=cleaned-parquet
    else
        format=parquet
    fi
    for ((j = ${start_run}; j <= ${num_runs}; j++)); do
        if [ $j -eq 0 ]; then
            expt=accuracy
        else
            expt=latency
        fi
        echo ">>> ${qdx}, run= ${j}, expt= ${expt}"
        expt_output_dir=saved-outputs/scale=${scale}/partition=${partition}/${format}/run=${j}
        expt_output_dir_ref=saved-outputs/scale=${scale}/partition=${partition}/${format}/run=0
        mkdir -p $expt_output_dir

        echo "Evicting Cache"
        vmtouch -e ${data_dir}/scale=${scale}/partition=${partition}/${format}

        query_output_dir=$expt_output_dir/q$qdx
        set -x
        RUST_LOG=warn ./tpch_polars \
            query q${qdx} ${scale} \
            ${data_dir}/scale=${scale}/partition=${partition}/${format} ${query_output_dir} ${expt} 2>>$expt_output_dir/results.csv
        set +x

        if [ $j -ne 0 ]; then
            echo "Extracting accuracy at run= ${j}"
            python3 scripts/extract_accuracy.py wake ${expt_output_dir} ${expt_output_dir_ref} ${qdx}
        fi
    done
done
