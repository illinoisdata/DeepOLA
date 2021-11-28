scale=10
mkdir -p data
cd tpch-dbgen
export DSS_PATH=../data
for chunk in {1..10}
	do
		./dbgen -f -C 10 -s $scale -S $chunk
	done

