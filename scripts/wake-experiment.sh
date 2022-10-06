if [ "$#" -ne 3 ]; then
	echo "Usage: ./wake-experiment.sh <scale> <partition> <log-level=INFO/WARN>"
	exit
fi
scripts_dir=$PWD
cd ../deepola/wake/
scale=$1
partition=$2
log_level=$3
output_dir=logs/scale\=$scale/partition\=$partition/
mkdir -p $output_dir
echo "Running Queries"
for query_no in {1..22}
do
	query=q$query_no
	sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
	vmtouch -e /mnt/DeepOLA/resources/tpc-h/data/scale\=$scale/partition\=$partition/parquet/*
	RUST_LOG=$log_level cargo run --release --example tpch_polars -- query $query $scale /mnt/DeepOLA/resources/tpc-h/data/scale\=$scale/partition\=$partition/parquet/ > logs/scale\=$scale/partition\=$partition/$query-$log_level.log
done
cd $scripts_dir
python3 obtain-time-taken.py $scale $partition $log_level

# Add code for obtaining error time-series
# Add code for cardinality vs time vs error information
# Add code for obtaining peak memory usage
# Add code for generating node wise processing time
