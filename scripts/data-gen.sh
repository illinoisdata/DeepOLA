scale=10
partition=1
cd ../
mkdir -p data/scale=$scale/partition=$partition
export DSS_PATH=$PWD/data/scale=$scale/partition=$partition
echo $DSS_PATH
cd tpch-kit/dbgen/
if [ $partition -eq 1 ];
then
	echo "Running"
	./dbgen -f -s $scale -v
else
	for chunk in $(seq 1 $partition)
		do
			echo $chunk
			./dbgen -f -s $scale -S $chunk -C $partition -v
		done
fi
