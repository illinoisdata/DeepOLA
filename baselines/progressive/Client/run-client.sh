if [ "$#" -ne 3 ]; then
	echo "Usage: ./run-client.sh <scale> <variation> <num-runs>"
	exit
fi

scale=$1
variation=$2
num_runs=$3

echo "CREATING DATA PARTITION"
mvn compile exec:java -Dexec.mainClass="progressive.load.Main"

output_dir=outputs/scale=$scale
mkdir -p $output_dir

for run in $( seq 1 $num_runs )
do
	echo "RUNNING QUERY Q1"
	q1_output=outputs/scale=$scale/$variation/run=$run/q1
	mkdir -p $q1_output
	mvn compile exec:java -Dexec.mainClass="progressive.query.one.Main" > $q1_output/run.log

	q6_output=outputs/scale=$scale/$variation/run=$run/q6
	mkdir -p $q6_output
	echo "RUNNING QUERY Q6"
	mvn compile exec:java -Dexec.mainClass="progressive.query.six.Main" > $q6_output/run.log
done
