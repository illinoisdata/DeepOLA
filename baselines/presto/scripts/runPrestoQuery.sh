#Update accordingly
MY_DIR='/home/aravmc2/hadoop'
QUERY_FILE='/home/aravmc2/DeepOLA/baselines/presto/queries'
queryNum=1

for FILE in $QUERY_FILE/*; 
do
    echo "This is the file we are running $FILE" >> $MY_DIR/timeFile;  
    for i in {1..10}; 
    do

        #This should clear the OS cache
        #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'
        TIMEFORMAT=%lR;
        { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file $FILE >> $MY_DIR/outputFile; } 2>> $MY_DIR/timeFile;

        #Note have to clear outputFile otherwise the files can causes issues when trying to open it due to the size
        >  $MY_DIR/outputFile;
        #Remove this line if you want the query output

        unset TIMEFORMAT;
    done
    let queryNum=queryNum+1;
done