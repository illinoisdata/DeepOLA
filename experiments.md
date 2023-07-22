# Experiments

## Data Generation

### Generate Locally (Optional)
TPC-H (Scale 100, Partition 100)
```bash
docker build -t deepola-wake:sigmod2023-data -f dockerfiles/data.Dockerfile .
docker run -i -v ./resources:/dataset/resources:rw -t deepola-wake:sigmod2023-data
./data-gen.sh 100 100 /dataset
```

TODO: Automatically generate parquet and cleaned-parquet formats as well (as part of data-gen.sh)

TODO: Generate dataset for depth experiment

## Installation

TODO: Pull docker images

### Build Locally (Optional)

```bash
docker build -t deepola-wake:sigmod2023 -f dockerfiles/wake.Dockerfile .
```

## TPC-H Benchmark (Figures 7 and 8)

Experiment results for each method will be saved under `results/<method>`.

Polars (scale 100, partition 100, 10 runs, Q1-Q22):
```bash
DATA_DIR=/absolute/path/to/data # containing scale=100/partition=100/parquet
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    -v `pwd`/results/polars:/outputs/polars \
    --name polars deepola-wake:sigmod2023-polars \
    bash experiment.sh /dataset/tpch /outputs/polars 100 100 10 1 1 22
```

Wake (scale 100, partition 100, 10 runs, Q1-Q22):
```bash
DATA_DIR=/absolute/path/to/data  # containing scale=100/partition=100/[parquet|cleaned_parquet]
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset/tpch 100 100 10 0 1 22
```

## Comparison with OLA Systems (Figure 9)

Wake (scale 100, partition 100, 10 runs, Q23-Q27):
```bash
DATA_DIR=/absolute/path/to/data  # containing scale=100/partition=100/[parquet|cleaned_parquet]
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset/tpch 100 100 10 0 23 27
```

## Confidence Interval (Figure 10)

Wake (scale 100, partition 100, 10 runs, Q14):
```bash
DATA_DIR=/absolute/path/to/data  # containing scale=100/partition=100/[parquet|cleaned_parquet]
docker run --rm \
    -v ${DATA_DIR}:/dataset/tpch:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_ci.sh /dataset/tpch 100 100 10 0
```

## Impact of Query Depth (Figure 11)

## Impact of Partition Size (Figure 12)
