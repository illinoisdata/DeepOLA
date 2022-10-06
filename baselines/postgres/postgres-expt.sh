scale=$1
partition=$2
num_runs=$3
echo "[1/3] SETTING UP POSTGRES CONTAINER"
./1-setup-postgres.sh $scale $partition
echo "[2/3] OBTAINING CORRECT OUTPUTS"
./2-obtain-correct-output.sh $scale
./2a-extract-output.sh $scale
echo "[3/3] TIMING RESULTS"
./3-time-queries.sh $scale $num_runs
python3 3a-extract-time.py $scale $num_runs
