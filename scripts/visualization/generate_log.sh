query=$1
scale=$2
partition=$3
data_dir=$4
cwd=$PWD
mkdir -p ../../deepola/wake/logs/scale=$scale/partition=$partition
cd ../../deepola/wake/
echo "Clearing Cache"
sync; echo 3 > sudo tee /proc/sys/vm/drop_caches
echo "Running Query"
RUST_LOG=info cargo run --release --example tpch_polars -- query $query $scale $data_dir/resources/tpc-h/data/scale=$scale/partition=$partition/parquet > logs/scale=$scale/partition=$partition/$query.log
echo "Generating Plot"
cd $cwd
python3 process_logs.py $query $scale $partition
