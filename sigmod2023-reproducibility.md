# SIGMOD 2023 Reproducibility Instructions

## Setup

Prequisite:
- Bash
- [Docker](https://docs.docker.com/get-docker/)

Clone the repository:
```
git clone https://github.com/illinoisdata/DeepOLA.git
cd DeepOLA
```

## Run All

The following script generates the dataset in the `$(pwd)/datasets`, runs the various baselines and Wake (our work), and generates the figures in the `$(pwd)/results/viz` directory with the individual baseline results stored in `$(pwd)/results/<method>`. This should take 2-3 hours to complete.
```bash
bash master_script.sh $(pwd)/datasets 10 10 10
```

To run full-scale experiments shown in the paper, run on scale 100 with 100 partitions.
```bash
bash master_script.sh $(pwd)/datasets 100 100 10
``` 

## Run in Detail

Alternatively, you can run each command separately from the description below. Ensure that the experiment parameters have been set consistently.

### Download Docker Images

Pull built Docker images using the following commands.
```bash
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
```

Optionally, they can be built locally by following the instruction below.

#### Build Docker Images Locally (Optional)

```bash
docker build -t deepola-data:sigmod2023 -f dockerfiles/data.Dockerfile .
docker build -t deepola-polars:sigmod2023 -f dockerfiles/polars.Dockerfile .
docker build -t deepola-wanderjoin:sigmod2023 -f dockerfiles/wanderjoin.Dockerfile .
docker build -t deepola-wake:sigmod2023 -f dockerfiles/wake.Dockerfile .
docker build -t deepola-viz:sigmod2023 -f dockerfiles/viz.Dockerfile .
```

### Set Experiment Parameters

#### Setup Data Directory

Set the directory where all datasets will be stored. The path must be an absolute path.
```bash
export DATA_DIR=`pwd`/datasets
mkdir -p ${DATA_DIR}
```

#### Setup Experiment Parameters. Scale represents roughly the storage size in GB.

- Since the end-to-end experiments might take significant time and require large memory (for some baselines), you can run a smaller scale factor with a smaller number of partitions.
```bash
export SCALE=10
export PARTITION=10
export NUM_RUNS=10
```

- For full-scale experiments presented in the paper.
```bash
export SCALE=100
export PARTITION=100
export NUM_RUNS=10
```

### TPC-H Data Generation

TPC-H (Scale `SCALE`, Partition `PARTITION`)
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    bash data-gen.sh ${SCALE} ${PARTITION} /dataset/tpch

docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    python3 convert-to-parquet.py /dataset/tpch/scale=${SCALE}/partition=${PARTITION}/tbl
```

### ProgressiveDB Dataset Generation
- Note: Ensure that the data has been generated already. This script only converts `lineitem` table to `cleaned-tbl` format.
- For TPC-H (Scale `SCALE`, Partition `PARTITION`)
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    python3 clean-data.py /dataset/tpch ${SCALE} ${PARTITION}
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    python3 convert-to-parquet.py /dataset/tpch/scale=${SCALE}/partition=${PARTITION}/cleaned-tbl
```

### Depth Experiment Dataset Generation (Figure 11)

```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    python scripts/deep_data_gen.py 10 1000000 100 4 /dataset/g10_p1m_n100_c4
```

### TPC-H Benchmark (Figures 7 and 8)

Experiment results for each method will be saved under `results/<method>`.

#### Postgres (scale `SCALE`, partition `PARTITION`, runs `NUM_RUNS`, Q1-Q22):

- Setup Postgres:
```bash
export QUERY_DIR=./resources/tpc-h/queries
export POSTGRES_DIR=./tmp/postgres/scale=${SCALE}/partition=${PARTITION}
./baselines/postgres/experiment-setup.sh ${DATA_DIR} ${QUERY_DIR} ${POSTGRES_DIR} ${SCALE} ${PARTITION}
```

- Run Queries:
```bash
export QUERY_DIR=./resources/tpc-h/queries
export OUTPUT_DIR=./results/postgres/scale=${SCALE}/
export POSTGRES_DIR=./tmp/postgres/scale=${SCALE}/partition=${PARTITION}
./baselines/postgres/experiment-time.sh ${QUERY_DIR} ${OUTPUT_DIR} ${POSTGRES_DIR} ${SCALE} ${PARTITION} ${NUM_RUNS} 1 1 22
python3 baselines/postgres/extract-time.py ${OUTPUT_DIR} ${SCALE} ${PARTITION} ${NUM_RUNS} 1 1 22 > ${OUTPUT_DIR}/timings.csv
```

#### Polars (scale `SCALE`, partition `PARTITION`, runs `NUM_RUNS`, Q1-Q22):
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    -v `pwd`/results/polars:/results/polars \
    --name polars deepola-polars:sigmod2023 \
    bash experiment.sh /dataset/tpch /results/polars ${SCALE} ${PARTITION} ${NUM_RUNS} 1 1 22
```

#### Wake (scale `SCALE`, partition `PARTITION`, runs `NUM_RUNS`, Q1-Q22):
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset ${SCALE} ${PARTITION} ${NUM_RUNS} 0 1 22
```

Then visualize the experiment results using the following commands.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/polars:/results/polars:rw \
    -v `pwd`/results/postgres:/results/postgres:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch.py ${SCALE} ${PARTITION} ${NUM_RUNS}
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/polars:/results/polars:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch_error.py ${SCALE} ${PARTITION} ${NUM_RUNS}
```

Figures will appear at `./results/viz/fig7_tpch.png` and `./results/viz/fig8_tpch_error.png`.


### Comparison with OLA Systems (Figure 9)

#### Wake (scale `SCALE`, partition `PARTITION`, runs `NUM_RUNS`, Q23-Q27):
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset ${SCALE} ${PARTITION} ${NUM_RUNS} 0 23 27
```

#### Wanderjoin (scale `SCALE`, partition `PARTITION`, runs `NUM_RUNS`, Q23-Q25):
```bash
docker run --rm \
    -v ${DATA_DIR}:/wanderjoin/tpch:rw \
    -v `pwd`/results/wanderjoin:/wanderjoin/outputs:rw \
    --name wanderjoin deepola-wanderjoin:sigmod2023 \
    bash experiment.sh tpch queries outputs ${SCALE} ${PARTITION} ${NUM_RUNS} 1 23 25
```

Then visualize the experiment results using the following command.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/wanderjoin:/results/wanderjoin:rw \
    -v `pwd`/results/polars:/results/polars:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch_ola.py ${SCALE} ${PARTITION} ${NUM_RUNS}
```

Figure will appear at `./results/viz/fig9_tpch_ola.png`.

### Confidence Interval (Figure 10)

Wake (scale `SCALE`, partition `PARTITION`, runs 100, Q14):
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_ci.sh /dataset ${SCALE} ${PARTITION} 100 0
```

Then visualize the experiment results using the following command.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_ci.py ${SCALE} ${PARTITION} 100
```

Figure will appear at `./results/viz/fig10_tpch_ola.png`.

### Impact of Query Depth (Figure 11)

After generating dataset using `scripts/deep_data_gen.py` earlier, the following commands tests Wake on depth 0-10.
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_depth.sh /dataset 10 0
```

Then visualize the experiment results using the following command.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_depth.py 10
```

Figure will appear at `./results/viz/fig11_depth.png`.