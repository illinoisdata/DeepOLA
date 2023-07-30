#/!bin/bash

int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 3 ]; then
    echo "Expected 3 arguments, $# given."
    echo "Usage: ./experiment.sh <data_dir> <scale-factor> <partition>"
    exit
fi

data_dir=$1
scale=$2
partition=$3
file_path="${data_dir}/scale=${scale}/partition=${partition}/tbl/"
DB="wander_${scale}_${partition}"

## Create database
createdb $DB

psql -d $DB -c "CREATE SCHEMA IF NOT EXISTS wanderjoin"
psql -d $DB -f "setup-queries/create-tables.sql"

for tbl in region nation customer supplier part partsupp orders lineitem
do
    cat $file_path/$tbl.tbl* | psql -d $DB -c "\copy $tbl FROM stdin WITH (FORMAT csv, DELIMITER '|')"
done

psql -d $DB -f "setup-queries/create-indexes.sql"
