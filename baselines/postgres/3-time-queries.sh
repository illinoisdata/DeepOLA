if [ "$#" -ne 3 ]; then
	echo "Usage: ./3-time-queries.sh <scale> <start-run> <num-runs>"
	exit
fi
path="$(git rev-parse --show-toplevel)" #path to main directory
scale=$1
start_run=$2
num_runs=$3
container=pgsql-$scale
query_dir=$path/resources/tpc-h/queries
output_dir=$path/baselines/postgres/outputs/scale=$scale/
mkdir -p $output_dir
echo "Output Directory $output_dir"

echo "Running Queries"
for run in $( seq $start_run $num_runs )
do
	echo "Postgres run $run"
	for query_no in {1..22}
	do
		echo "Clearing Cache"
		sudo docker exec -it pgsql-$scale sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
		echo "Executing $query_no"
		sudo docker exec -it pgsql-$scale psql -U postgres -c "\pset pager off" -c "\timing" -f "$query_dir/$query_no.sql" > $output_dir/q$query_no-run$run.log
	done
done
