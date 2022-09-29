query=$1
scale=$2
partition=$3
mkdir -p ../../deepola/wake/logs/scale=$scale/partition=$partition
cd ../../deepola/wake/
RUST_LOG=info cargo run --release --example tpch_polars -- query $query $scale resources/tpc-h/data/scale=$scale/partition=$partition > logs/scale=$scale/partition=$partition/$query.log
