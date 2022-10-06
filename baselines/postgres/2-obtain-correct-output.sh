if [ "$#" -ne 1 ]; then
	echo "Usage: ./2-obtain-correct-output.sh <scale>"
	exit
fi
# Assume that a container pgsql-$scale is running with data loaded and queries ready to be executed.
# If not, then first create and setup the container using `setup-postgres.sh`
# If the container was already setup but shutdown, restart container using `restart-postgres.sh`
scale=$1
container=pgsql-$scale
query_dir=/home/deepola/DeepOLA/resources/tpc-h/queries
output_dir=/mnt/DeepOLA/resources/tpc-h/data/scale\=$scale/original/
mkdir -p $output_dir
echo "Output Directory $output_dir"
echo "Running Queries"
for query_no in {1..22}
do
	echo "Executing $query_no"
	# Q15 needs special handling since it involves create view, run query and drop view.
	if [ $query_no -eq 15 ]; then
		sudo docker exec -it pgsql-$scale psql -U postgres -f $query_dir/15a.sql #CREATE VIEW
		sql_query=$(<$query_dir/15b.sql)
		query="${sql_query//;}"
		sudo docker exec -it pgsql-$scale psql -U postgres -c "COPY ($query) TO '/var/lib/postgresql/data/$query_no.csv' WITH CSV DELIMITER ',' HEADER;" #RUN QUERY
		sudo docker exec -it pgsql-$scale psql -U postgres -f $query_dir/15c.sql #DROP VIEW
	else
		sql_query=$(<$query_dir/$query_no.sql)
		query="${sql_query//;}"
		sudo docker exec -it pgsql-$scale psql -U postgres -c "COPY ($query) TO '/var/lib/postgresql/data/$query_no.csv' WITH CSV DELIMITER ',' HEADER;"
	fi
done
