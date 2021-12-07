scale=10
partition=10
mkdir -p data/scale=$scale/partition=$partition
export DSS_PATH=$PWD/data/scale=$scale/partition=$partition
echo $DSS_PATH
cd tpch-kit/dbgen/
for chunk in $(seq 1 $partition)
	do
		echo $chunk
		./dbgen -f -s $scale -S $chunk -C $partition -v
	done

