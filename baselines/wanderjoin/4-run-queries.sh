if [ "$#" -ne 4 ]; then
   echo "Usage: ./4-run-queries.sh <scale: 10/100> <variation: data/skewed-data> <num_runs> <use_cache: 0/1>"
   exit
fi
set -o xtrace
SCALE=$1
DATA_VARIATION=$2
NUM_RUNS=$3
USE_CACHE=$4
DB="wander_${SCALE}_var_${DATA_VARIATION}"
PSQL_BIN_DIR=/usr/local/pgsql/bin
PSQL=$PSQL_BIN_DIR/psql
QUERY_FOLDER=queries
if [ $USE_CACHE -eq 1 ]; then
    echo "Scanning Tables"
    ${PSQL} -d ${DB} -f "setup-queries/scan-tables.sql"
fi

for RUN in $( seq 1 $NUM_RUNS )
do
    OUTPUT_DIR=outputs/scale=$SCALE/variation=$DATA_VARIATION/run=$RUN
    mkdir -p $OUTPUT_DIR

    for query in $QUERY_FOLDER/*.sql
    do
        query_name=$(basename ${query})
        if [ $USE_CACHE -ne 1 ]; then
            ${PSQL} -d ${DB} -c "DISCARD ALL"
        fi
        ${PSQL} -d ${DB} -f ${query} -o ${OUTPUT_DIR}/${query_name}-cache-${USE_CACHE}.csv -F ',' -A
    done
done
