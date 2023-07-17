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

## Installation

TODO: Pull docker images

### Build Locally (Optional)

```bash
docker build -t deepola-wake:sigmod2023 -f dockerfiles/wake.Dockerfile .
```

## TPC-H Benchmark (Figures 7 and 8)

Experiment results for each method will be saved under `results/<method>`.

Wake (scale 100, partition 100, 10 runs, Q1-Q22):
```bash
DATA_DIR=/absolute/path/to/data  # containing scale=100/partition=100/[parquet|cleaned_parquet]
docker run --rm \
    -v ${DATA_DIR}:/dataset:rw \
    -v `pwd`/results/wake:/saved-outputs:rw \
    --name wake deepola-wake:sigmod2023 \
    bash scripts/experiment_wake_tpch.sh /dataset 100 100 10 0 1 22
```

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

## Impact of Partition Size (Figure 12)
