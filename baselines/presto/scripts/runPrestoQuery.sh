#Update accordingly
MY_DIR='/home/aravmc2/hadoop'

echo "This is query 1" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ1.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 2" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ2.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 3" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ3.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 4" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ4.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 5" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ5.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 6" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ6.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 7" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ7.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 8" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ8.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 9" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ9.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 10" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ10.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 11" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ11.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 12" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ12.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 13" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ13.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 14" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ14.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 15" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ15.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 16" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ16.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 17" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ17.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 18" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ18.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 19" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ19.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 20" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ20.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 21" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ21.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done

echo "This is query 22" >> $MY_DIR/timeFile;    
for i in {1..10}; 
do

    #This should clear the OS cache
    #sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

    echo "This is attempt $i" >> $MY_DIR/output-file;    
    TIMEFORMAT=%lR;
    ##REPLACE THE -file param with the location of the query that you want to test
    { time /data/tpch_data/presto-server-0.270/bin/presto --server localhost:8090 --catalog hive --schema tpch10gb --file /home/aravmc2/DeepOLA/baselines/presto/queries/PrestoQ22.sql >> ~/hadoop/output-file; } 2>> $MY_DIR/timeFile;
    echo "" >>  $MY_DIR/output-file;
    unset TIMEFORMAT;
done