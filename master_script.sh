#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 4 ]; then
    echo "Expected 4 arguments, $# given."
    echo "Usage: ./master_script.sh <data_dir> <scale> <partitions> <num_runs>"
    exit
fi

DATA_DIR=$1
mkdir -p ${DATA_DIR}
SCALE=$2
PARTITION=$3
NUM_RUNS=$4

set -x

echo "[*] Pulling Docker images"
docker pull supawit2/deepola-data:sigmod2023
docker pull supawit2/deepola-polars:sigmod2023
docker pull supawit2/deepola-wanderjoin:sigmod2023
docker pull supawit2/deepola-wake:sigmod2023
docker pull supawit2/deepola-viz:sigmod2023
docker image tag supawit2/deepola-data:sigmod2023 deepola-data:sigmod2023
docker image tag supawit2/deepola-polars:sigmod2023 deepola-polars:sigmod2023
docker image tag supawit2/deepola-wanderjoin:sigmod2023 deepola-wanderjoin:sigmod2023
docker image tag supawit2/deepola-wake:sigmod2023 deepola-wake:sigmod2023
docker image tag supawit2/deepola-viz:sigmod2023 deepola-viz:sigmod2023

echo "[*] Generating Dataset"
# Generate TPC-H Dataset
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    bash data-gen.sh ${SCALE} ${PARTITION} /dataset/tpch
# Convert to Parquet
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    python3 convert-to-parquet.py /dataset/tpch/scale=${SCALE}/partition=${PARTITION}/tbl

# Generate Cleaned Table Dataset
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    python3 clean-data.py /dataset/tpch ${SCALE} ${PARTITION}

# Convert to Parquet
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    python3 convert-to-parquet.py /dataset/tpch/scale=${SCALE}/partition=${PARTITION}/cleaned-tbl

echo "[*] Figure 7: Existing Exact Baselines"
# Running Postgres
export QUERY_DIR=./resources/tpc-h/queries
export POSTGRES_DIR=./tmp/postgres/scale=${SCALE}/partition=${PARTITION}
export OUTPUT_DIR=./results/postgres/scale=${SCALE}/
./baselines/postgres/experiment-setup.sh ${DATA_DIR} ${QUERY_DIR} ${POSTGRES_DIR} ${SCALE} ${PARTITION}
./baselines/postgres/experiment-time.sh $QUERY_DIR $OUTPUT_DIR $POSTGRES_DIR ${SCALE} ${PARTITION} ${NUM_RUNS} 1 1 22
python3 baselines/postgres/extract-time.py $OUTPUT_DIR ${SCALE} ${PARTITION} ${NUM_RUNS} 1 1 22 > $OUTPUT_DIR/timings.csv

# Running Polars
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    -v `pwd`/results/polars:/results/polars \
    --name polars deepola-polars:sigmod2023 \
    bash experiment.sh /dataset/tpch /results/polars ${SCALE} ${PARTITION} ${NUM_RUNS} 1 1 22

# Running WAKE
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset ${SCALE} ${PARTITION} ${NUM_RUNS} 0 1 22

# Visualizing the results to obtain Figure 7.
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/polars:/results/polars:rw \
    -v `pwd`/results/postgres:/results/postgres:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch.py ${SCALE} ${PARTITION} ${NUM_RUNS}

# Visualizing the results to obtain Figure 8.
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/polars:/results/polars:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch_error.py ${SCALE} ${PARTITION} ${NUM_RUNS}


echo "[*] Figure 9: Existing OLA Baselines"
# Running Wake
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset ${SCALE} ${PARTITION} ${NUM_RUNS} 0 23 27

# Running Wanderjoin
docker run --rm \
    -v ${DATA_DIR}:/wanderjoin/tpch:rw \
    -v `pwd`/results/wanderjoin:/wanderjoin/outputs:rw \
    --name wanderjoin deepola-wanderjoin:sigmod2023 \
    bash experiment.sh tpch queries outputs ${SCALE} ${PARTITION} ${NUM_RUNS} 1 23 25

# Visualizing results
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/wanderjoin:/results/wanderjoin:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    -v `pwd`/results/polars:/results/polars:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch_ola.py ${SCALE} ${PARTITION} ${NUM_RUNS}


echo "[*] Figure 10: Confidence Interval"
# Running Wake
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_ci.sh /dataset ${SCALE} ${PARTITION} 100 0

# Visualizing Results
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_ci.py ${SCALE} ${PARTITION} 100


echo "[*] Figure 11: Query Depth Experiment"
# Generate Dataset
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    python scripts/deep_data_gen.py 10 1000000 100 4 /dataset/g10_p1m_n100_c4

# Running Wake
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_depth.sh /dataset 10 0

# Visualizing Results
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_depth.py 10

set +x