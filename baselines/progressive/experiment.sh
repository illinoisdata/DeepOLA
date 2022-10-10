set -o xtrace

if [ "$#" -ne 4 ]; then
	echo "Usage; ./experiment.sh <scale> <partition> <variation> <num-runs>"
	exit
fi

scale=$1
partition=$2
variation=$3
num_runs=$4

echo "Trying to stop existing progressive-db"
pid=$(pidof java)
kill -s TERM $pid
echo "Removing Current SQLITE"
rm progressivedb.sqlite

echo "Running ProgressiveDB Server"
nohup java -jar progressive-db.jar &

echo "Starting Main EXPERIMENT"
./1-setup-postgres.sh $scale $partition $variation
cd Client
./run-client.sh $scale $variation $num_runs
python3 extract-results.py $scale $variation $num_runs

echo "Experiment Run Finished"
echo "Removing Postgres Directory"
./clean-setup.sh $scale $variation
