scale=$1
partition=$2
cd ../
export DSS_PATH=$PWD/resources/tpc-h/data/scale=$scale/partition=$partition
mkdir -p $DSS_PATH
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
