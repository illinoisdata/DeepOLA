for i in {1..10}; 
do    
    # Ouput-file should be query output and timerFile should be the list of times
    # Make sure to change the locations of the files
    echo "This is attempt $i" >>  ~/hadoop/output-file;    
    TIMEFORMAT=%lR;
    ## Replace the -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8080 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ1.sql >> ~/hadoop/output-file; } 2>> ~/hadoop/timeFile;
    echo "" >>  ~/hadoop/output-file;
    unset TIMEFORMAT;
done