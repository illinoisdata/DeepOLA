#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

docker build -t deepola-data:sigmod2023 -f dockerfiles/data.Dockerfile .
docker build -t deepola-polars:sigmod2023 -f dockerfiles/polars.Dockerfile .
docker build -t deepola-wanderjoin:sigmod2023 -f dockerfiles/wanderjoin.Dockerfile .
docker build -t deepola-wake:sigmod2023 -f dockerfiles/wake.Dockerfile .
docker build -t deepola-viz:sigmod2023 -f dockerfiles/viz.Dockerfile .
