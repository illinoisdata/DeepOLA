if [ "$#" -ne 3 ]; then
	echo "Usage: ./experiment.sh <scale-factor> <partition> <num-runs>"
	exit
fi

scale=$1
partition=$2
num_runs=$3

## Set polars parameters to disable any additional logging
export SCALE_FACTOR=$scale
export PARTITION=$partition
export DATA_BASE_DIR=/mnt/DeepOLA/
export INCLUDE_IO=1
#export SHOW_PLAN=0
#export SHOW_RESULTS=0
#export LOG_TIMINGS=0
OUTPUT_DIR=outputs/scale=$scale/partition=$partition
mkdir -p $OUTPUT_DIR

## Create virtualenv to execute polars queries
make .venv

## Perform Benchmarking for Time
echo "query,run,time,peak_mem" >> $OUTPUT_DIR/timings.csv
for run in $(seq 0 $num_runs)
do
	echo "Experiment Run: $run"
	for query in {1..23}
	do
		if [ $run -ne 0 ]; then
			echo "Running Query: $query"
			sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
			sleep 2
			/usr/bin/time -f "$query,$run,%e,%M" ./.venv/bin/python -m polars_queries.q$query 2>>$OUTPUT_DIR/timings.csv
		else
			export SAVE_RESULTS=1
			echo "Running Save Output Query: $query"
			./.venv/bin/python -m polars_queries.q$query
			export SAVE_RESULTS=0
		fi
	done
done
