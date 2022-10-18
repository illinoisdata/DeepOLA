sudo apt install -y make
sudo apt install -y build-essential

if [ ! -d "TPC-H-Skew" ]
then
	git clone https://github.com/YSU-Data-Lab/TPC-H-Skew
	cd TPC-H-Skew
	make
else
	echo "Repository Already Cloned"
	cd TPC-H-Skew
fi

echo "Generating TPC-H Skewed Dataset"
scale=$1
partition=$2
z=1
DATA_DIR=/data/DeepOLA
export DSS_PATH=$DATA_DIR/resources/tpc-h/skewed-data/scale=$scale/partition=$partition/tbl
echo "Starting Data Generation"
echo $DSS_PATH
mkdir -p $DSS_PATH

if [ $partition -ge 2 ]; then
	echo "Generating Seed-Set"
	./dbgen -f -z $z -s $scale -C $partition -O s
	./dbgen -f -z $z -s $scale -C $partition -v
	rm $DSS_PATH/order.tbl
	rm $DSS_PATH/part.tbl
	rm $DSS_PATH/supplier.tbl
	rm $DSS_PATH/customer.tbl
	time ./dbgen -f -z $z -s $scale -C $partition -T O -v
	rm $DSS_PATH/order.tbl
else
	./dbgen -f -z $z -s $scale
fi
