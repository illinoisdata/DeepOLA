#Update accordingly
MY_DIR='/home/awsuser/QueryResults'
QUERY_FILE='/home/awsuser/DeepOLA/baselines/presto/queries'

for FILE in $QUERY_FILE/*; 
do
    echo "This is the file we are running $FILE" >> $MY_DIR/timeFile;  
    for i in {1..10}; 
    do

        #This should clear the OS cache
        #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'
        TIMEFORMAT=%lR;
        { time /usr/local/pgsql/bin/psql -d test -U postgres -f $FILE > $MY_DIR/outputFile; } 2>> $MY_DIR/timeFile;

        #Note have to clear outputFile otherwise the files can causes issues when trying to open it due to the size
        >  $MY_DIR/outputFile;
        #Remove this line if you want the query output

        unset TIMEFORMAT;
    done
done
