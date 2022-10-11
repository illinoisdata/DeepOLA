POSTGRES_BASE_DIR=/mnt/wander-postgres/
POSTGRES_DATA_DIR=$POSTGRES_BASE_DIR/data
POSTGRES_LOG_FILE=$POSTGRES_BASE_DIR/logfile

echo """A script for setup instructions to be added.
Currently, follow the readme information from XDB, to compile the postgres binary.
Once the binary is compiled, start the postgres server with the data directory correctly specified.
Along with this, we want the checkpoint_segments parameter to be set to max to allow for quickly loading data.

Now, switch to postgres user (create if doesn't exist). Create a role named deepola.
/usr/local/pgsql/bin/psql -c "CREATE ROLE deepola SUPERUSER";
/usr/local/pgsql/bin/psql -c "ALTER ROLE 'deepola' WITH LOGIN";

Now, back from deepola user, you should be able to execute queries
/usr/local/pgsql/bin/createdb deepola;
/usr/local/pgsql/bin/psql -c "";
/usr/local/pgsql/bin/psql -f "";

This helps in being able to access the data files and queries to be executed.
Note, this steps need to be done once for all the experiments.
"""