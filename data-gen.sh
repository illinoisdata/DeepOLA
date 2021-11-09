cd tpch-kit/dbgen
export DSS_PATH=../../data
for chunk in {1..10}
	do
		./dbgen -f -C 10 -S $chunk
	done

