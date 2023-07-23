# Experiments

## Setup

### Build Locally (Optional)

TODO: Pull docker images

```bash
docker build -t deepola-data:sigmod2023 -f dockerfiles/data.Dockerfile .
docker build -t deepola-polars:sigmod2023 -f dockerfiles/polars.Dockerfile .
docker build -t deepola-wake:sigmod2023 -f dockerfiles/wake.Dockerfile .
docker build -t deepola-viz:sigmod2023 -f dockerfiles/viz.Dockerfile .
```

### Local Data Generation

#### Generate Dataset

TPC-H (Scale 100, Partition 100)
```bash
export DATA_DIR=/absolute/path/to/data  # where you want to put scale=100/partition=100/[tbl|parquet]
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    --name dataset deepola-data:sigmod2023 \
    bash data-gen.sh 100 100 /dataset/tpch
```

TODO: Automatically generate cleaned-parquet formats as well (as part of data-gen.sh)

### Generate Dataset for Depth Experiment (Figure 11)

```bash
DATA_DIR=/absolute/path/to/data
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    python scripts/deep_data_gen.py 10 1000000 100 4 /dataset/g10_p1m_n100_c4
```

Deep SQL queries can be obtained by following command. This is optional for testing other baselines and not necessary for testing Wake.
```
python scripts/deep_query_gen.py
```

## TPC-H Benchmark (Figures 7 and 8)

Experiment results for each method will be saved under `results/<method>`.

Polars (scale 100, partition 100, 10 runs, Q1-Q22):
```bash
DATA_DIR=/absolute/path/to/data # containing scale=100/partition=100/parquet
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    -v `pwd`/results/polars:/outputs/polars \
    --name polars deepola-polars:sigmod2023 \
    bash experiment.sh /dataset/tpch /outputs/polars 100 100 10 1 1 22
```

Wake (scale 100, partition 100, 10 runs, Q1-Q22):
```bash
DATA_DIR=/absolute/path/to/data  # containing scale=100/partition=100/[parquet|cleaned_parquet]
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset 100 100 10 0 1 22
```

Then visualize the experiment results using the following commands.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch.py 100 100 10
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch_error.py 100 100 10
```

Figures will appear at `./results/viz/fig7_tpch.png` and `./results/viz/fig8_tpch_error.png`.


## Comparison with OLA Systems (Figure 9)

Wake (scale 100, partition 100, 10 runs, Q23-Q27):
```bash
DATA_DIR=/absolute/path/to/data  # containing scale=100/partition=100/[parquet|cleaned_parquet]
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset 100 100 10 0 23 27
```

Then visualize the experiment results using the following command.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_tpch_ola.py 100 100 10
```

Figure will appear at `./results/viz/fig9_tpch_ola.png`.

## Confidence Interval (Figure 10)

Wake (scale 100, partition 100, 10 runs, Q14):
```bash
DATA_DIR=/absolute/path/to/data  # containing scale=100/partition=100/[parquet|cleaned_parquet]
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_ci.sh /dataset 100 100 10 0
```

Then visualize the experiment results using the following command.
```bash
docker run --rm \
    -v `pwd`/results/wake:/results/wake:rw \
    -v `pwd`/results/viz:/results/viz:rw \
    --name viz deepola-viz:sigmod2023 \
    python3 scripts/plot_ci.py 100 100 10
```

Figure will appear at `./results/viz/fig10_tpch_ola.png`.

## Impact of Query Depth (Figure 11)

After generating dataset using `scripts/deep_data_gen.py` earlier, the following commands tests Wake on depth 0-10.
```bash
DATA_DIR=/absolute/path/to/data
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
