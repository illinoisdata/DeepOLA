if [ "$#" -ne 3 ]; then
	echo "Usage: ./time-queries.sh <scale> <start-run> <num-runs>"
	exit
fi
path="$(git rev-parse --show-toplevel)" #path to main directory
scale=$1
start_run=$2
num_runs=$3
query_dir=$path/resources/tpc-h/queries
output_dir=$path/baselines/vertica/outputs/scale=$scale
mkdir -p $output_dir
echo "Output Directory $output_dir"

echo "Running Queries"
for run in $( seq $start_run $num_runs )
do
	echo "Vertica run $run"
	run_output_dir=$output_dir/run=$run
	mkdir -p $run_output_dir

	for query_no in {1..22}
	do
		# Clearing both Kernel Cache and Vertica Internal Cache.
		echo "Clearing VSQL Cache"
		vsql -i -c "SELECT CLEAR_CACHES()"
		sudo $path/scripts/clear-cache.sh

		# Executing Queries
		echo "Executing $query_no"
		vsql -o $run_output_dir/$query_no.log -i -f $query_dir/$query_no.sql > $run_output_dir/$query_no-time.log
	done
done
