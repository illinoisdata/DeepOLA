# SCALE=10
# VARIATION="skewed-data"
# POSTGRES_BASE_DIR=/mnt/wander-postgres/$VARIATION/scale-$SCALE/
# POSTGRES_DATA_DIR=$POSTGRES_BASE_DIR/data
# POSTGRES_LOG_FILE=$POSTGRES_BASE_DIR/logfile
# cd flex-2.5.31
# ./configure --without-realine
# make
# sudo make install
# if ! id "postgres"; then
# 	sudo adduser postgres
# fi
# mkdir -p $POSTGRES_DATA_DIR
# sudo chown -r postgres $POSTGRES_DATA_DIR
# su - postgres
# PGSQL_BIN_DIR=/usr/lib/postgresql/10/bin
# $PGSQL_BIN_DIR/initdb -D $POSTGRES_DATA_DIR
# echo "Running Postgres"
# $PGSQL_BIN_DIR/postgres -D $POSTGRES_DATA_DIR > $POSTGRES_LOG_FILE 2>&1 &
# $PGSQL_BIN_DIR/createdb test
# $PGSQL_BIN_DIR/psql test
