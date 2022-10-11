if [ "$#" -ne 2 ]; then
   echo "Usage: ./3-load-data.sh <scale: 10/50/100> <variation: data/skewed-data>"
   exit
fi
set -o xtrace
SCALE=$1
DATA_VARIATION=$2
DB="wander_${SCALE}_var_${DATA_VARIATION}"
FILE_PATH=/mnt/DeepOLA/resources/tpc-h/$DATA_VARIATION/scale=$SCALE/partition=1/tbl
PSQL_BIN_DIR=/usr/local/pgsql/bin
CREATEDB=$PSQL_BIN_DIR/createdb
PSQL=$PSQL_BIN_DIR/psql

$CREATEDB $DB
$PSQL -d $DB -c "CREATE SCHEMA IF NOT EXISTS wanderjoin"
$PSQL -d $DB -f "setup-queries/create-tables.sql"
$PSQL -d $DB -c "COPY region FROM '$FILE_PATH/region.tbl' WITH delimiter '|' CSV;";
$PSQL -d $DB -c "COPY nation FROM '$FILE_PATH/nation.tbl' WITH delimiter '|' CSV;";
$PSQL -d $DB -c "COPY customer FROM '$FILE_PATH/customer.tbl' WITH delimiter '|' CSV;";
$PSQL -d $DB -c "COPY supplier FROM '$FILE_PATH/supplier.tbl' WITH delimiter '|' CSV;";
$PSQL -d $DB -c "COPY part FROM '$FILE_PATH/part.tbl' WITH delimiter '|' CSV;";
$PSQL -d $DB -c "COPY partsupp FROM '$FILE_PATH/partsupp.tbl' WITH delimiter '|' CSV;";
$PSQL -d $DB -c "COPY orders FROM '$FILE_PATH/orders.tbl' WITH delimiter '|' CSV;";
$PSQL -d $DB -c "COPY lineitem FROM '$FILE_PATH/lineitem.tbl' WITH delimiter '|' CSV;";
$PSQL -d $DB -f "setup-queries/create-index.sql"
$PSQL -d $DB -f "setup-queries/scan-tables.sql"
