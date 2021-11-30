scale=10
mkdir -p data
mkdir -p data/scale=$scale
cd tpch-kit/dbgen
export DSS_PATH=../../data/scale=$scale
for chunk in {1..10}
	do
		./dbgen -f -C 10 -s $scale -S $chunk
	done

