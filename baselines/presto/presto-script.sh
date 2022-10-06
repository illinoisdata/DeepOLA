if [ "$#" -ne 2 ]; then
	echo "Usage: ./presto-script.sh <scale> <num-runs>"
	exit
fi

scale=$1
num_runs=$2
query_files="queries"
presto_dir="/home/deepola/hadoop/presto-server-0.270/bin"
output_dir="outputs/scale=$scale"
mkdir -p $output_dir
for query_no in {1..22}
do
	echo "Query $query_no"
	TIMEFORMAT=%lR;
	for run in $( seq 1 $num_runs )
	do
		echo "Experiment Run $run"
		file=$query_files/PrestoQ$query_no.sql
		time $presto_dir/presto --server localhost:8090 --catalog hive --schema tpchs$scale --file $file --output-format CSV > $output_dir/$query_no.csv
		sleep 5
	done
done
unset TIMEFORMAT
