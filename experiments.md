# Experiments

## Setup

Prequisite:
- [Docker](https://docs.docker.com/get-docker/)

### Download Docker Images

Pull built Docker images using the following commands.
```bash
docker pull supawit2/deepola-data:sigmod2023
docker pull supawit2/deepola-polars:sigmod2023
docker pull supawit2/deepola-wake:sigmod2023
docker pull supawit2/deepola-viz:sigmod2023
docker image tag supawit2/deepola-data:sigmod2023 deepola-data:sigmod2023
docker image tag supawit2/deepola-polars:sigmod2023 deepola-polars:sigmod2023
docker image tag supawit2/deepola-wake:sigmod2023 deepola-wake:sigmod2023
docker image tag supawit2/deepola-viz:sigmod2023 deepola-viz:sigmod2023
```

Optionally, they can be built locally by following the optional instruction under "Build Docker Images Locally (Optional)" below.

### Parameter Setting

Set the directory where all datasets will be stored. The path must be an absolute path.
```bash
export DATA_DIR=`pwd`/experiment/dataset
mkdir -p ${DATA_DIR}
```

Select the dataset scale. This is roughly the storage size in GB.
```bash
export SCALE=100  # For full-scale experiments presented in the paper.
```

### TPC-H Data Generation

TPC-H (Scale 100, Partition 100)
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    bash data-gen.sh ${SCALE} 100 /dataset/tpch
```

TODO: Automatically generate cleaned-parquet formats as well (as part of data-gen.sh)

### Generate Dataset for Depth Experiment (Figure 11)

```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    python scripts/deep_data_gen.py 10 1000000 100 4 /dataset/g10_p1m_n100_c4
```

## TPC-H Benchmark (Figures 7 and 8)

Experiment results for each method will be saved under `results/<method>`.

### Postgres

- Setup Postgres (scale 100, partition 100):
```bash
export DATA_DIR=./resources/tpc-h/data
export QUERY_DIR=./resources/tpc-h/queries
export POSTGRES_DIR=./tmp/postgres/scale=100/partition=100
./baselines/postgres/experiment-setup.sh $DATA_DIR $QUERY_DIR $POSTGRES_DIR 100 100
```

- Run Queries (scale 100, partition 100, 10 runs, Q1-Q22):
```bash
export QUERY_DIR=./resources/tpc-h/queries
export OUTPUT_DIR=./outputs/postgres/scale=100/
export POSTGRES_DIR=./tmp/postgres/scale=100/partition=100
./baselines/postgres/experiment-time.sh $QUERY_DIR $OUTPUT_DIR $POSTGRES_DIR 100 100 10 1 1 22
python3 baselines/postgres/extract-time.py $OUTPUT_DIR 100 100 10 1 1 22 > $OUTPUT_DIR/results.csv
```

### Polars (scale 100, partition 100, 10 runs, Q1-Q22):
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    -v `pwd`/results/polars:/outputs/polars \
    --name polars deepola-polars:sigmod2023 \
    bash experiment.sh /dataset/tpch /outputs/polars ${SCALE} 100 10 1 1 22
```

### Wake (scale 100, partition 100, 10 runs, Q1-Q22):
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset ${SCALE} 100 10 0 1 22
```

Then visualize the experiment results using the following commands.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch.py ${SCALE} 100 10
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch_error.py ${SCALE} 100 10
```

Figures will appear at `./results/viz/fig7_tpch.png` and `./results/viz/fig8_tpch_error.png`.


## Comparison with OLA Systems (Figure 9)

Wake (scale 100, partition 100, 10 runs, Q23-Q27):
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset ${SCALE} 100 10 0 23 27
```

Then visualize the experiment results using the following command.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch_ola.py ${SCALE} 100 10
```

Figure will appear at `./results/viz/fig9_tpch_ola.png`.

## Confidence Interval (Figure 10)

Wake (scale 100, partition 100, 10 runs, Q14):
```bash
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_ci.sh /dataset ${SCALE} 100 10 0
```

Then visualize the experiment results using the following command.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_ci.py ${SCALE} 100 10
```

Figure will appear at `./results/viz/fig10_tpch_ola.png`.

## Impact of Query Depth (Figure 11)

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

## Impact of Partition Size (Figure 12)

TODO:

## Build Docker Images Locally (Optional)

```bash
docker build -t deepola-data:sigmod2023 -f dockerfiles/data.Dockerfile .
docker build -t deepola-polars:sigmod2023 -f dockerfiles/polars.Dockerfile .
docker build -t deepola-wake:sigmod2023 -f dockerfiles/wake.Dockerfile .
docker build -t deepola-viz:sigmod2023 -f dockerfiles/viz.Dockerfile .
```
