if [ "$#" -ne 2 ]; then
    echo "Usage: ./3-load-data.sh <scale: 10/50/100> <variation: data/skewed-data>"
    exit
fi
SCALE=$1
DATA_VARIATION=$2
FILE_PATH=/mnt/DeepOLA/resources/tpc-h/$DATA_VARIATION/scale=$SCALE/partition=1/tbl
echo "COPY region FROM '$FILE_PATH/region.tbl' WITH delimiter '|' CSV;";
echo "COPY nation FROM '$FILE_PATH/nation.tbl' WITH delimiter '|' CSV;";
echo "COPY customer FROM '$FILE_PATH/customer.tbl' WITH delimiter '|' CSV;";
echo "COPY supplier FROM '$FILE_PATH/supplier.tbl' WITH delimiter '|' CSV;";
echo "COPY part FROM '$FILE_PATH/part.tbl' WITH delimiter '|' CSV;";
echo "COPY partsupp FROM '$FILE_PATH/partsupp.tbl' WITH delimiter '|' CSV;";
echo "COPY orders FROM '$FILE_PATH/orders.tbl' WITH delimiter '|' CSV;";
echo "COPY lineitem FROM '$FILE_PATH/lineitem.tbl' WITH delimiter '|' CSV;";
