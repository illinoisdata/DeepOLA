query=$1
scale=$2
partition=$3
data_dir=$4
mkdir -p ../../deepola/wake/logs/scale=$scale/partition=$partition
cd ../../deepola/wake/
RUST_LOG=info cargo run --release --example tpch_polars -- query $query $scale $data_dir/resources/tpc-h/data/scale=$scale/partition=$partition/parquet > logs/scale=$scale/partition=$partition/$query.log
