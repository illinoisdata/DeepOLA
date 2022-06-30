#Update accordingly
MY_DIR='/home/awsuser/QueryResults'
QUERY_FILE='/home/awsuser/DeepOLA/baselines/wanderjoin/Queries/10online.sql'
DB_NAME='test2'

echo "This is the file we are running $FILE" >> $MY_DIR/timeFile;  
for i in {1..5}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    /usr/local/pgsql/bin/psql -d $DB_NAME -U postgres -f $QUERY_FILE >> $MY_DIR/ouputFile;

    #Note have to clear outputFile otherwise the files can causes issues when trying to open it due to the size
    #>  $MY_DIR/outputFile;
    #Remove this line if you want the query output

done
