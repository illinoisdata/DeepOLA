**Author:** Billy Li
**Last update:** May 4th, 2022


# Overview

This page is a brief overview on the (confusing) steps of setting up a Presto cluster on a Linux machine. It is assumed that the Presto cluster will use the Hive connector.

## Required installations

1. MySQL server
2. Hive Metastore
3. Presto SQL server and client

## Let's go!
### MySQL server
Download the MySQL docker image and start the MySQL server:

```
docker pull mysql/mysql-server:8.0  
docker run --name=mysql1 --restart on-failure -d mysql/mysql-server:8.0
```

Find the generated root user password and use it to login to the server:

```
docker logs mysql1 2>&1 | grep GENERATED\
[Entrypoint] GENERATED ROOT PASSWORD: IgI4i4t,&WvAG@pjC&8xr/08UX1@8=3^
docker exec -it mysql1 mysql -uroot -p
```

There is 1 variable which you need to find the value of for the next step (setting up Hive Metastore). However, you must change your password to something else before MySQL allows you to issue any other command:

```
ALTER USER 'root'@'localhost' IDENTIFIED BY 'verysecurepassword';
```

Make note of the ``system_time_zone`` variable of the MySQL server. If it is not UTC, there will be an additional step which you will need to perform when setting up Hive Metastore.

```
mysql> select @@system_time_zone;
+--------------------+
| @@system_time_zone |
+--------------------+
| CDT                |
+--------------------+
1 row in set (0.00 sec)
```

### Hive Metastore
Hive Metastore requires 2 installations to run:
   - Hive installation: wget https://downloads.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
   - Hadoop installation: wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz

You now need to edit the Hive config files to tell Hive where itself and the Hadoop installation is located:
```
$ cd SOMEWHERE/apache-hive-3.1.2-bin.tar.gz/bin
$ vim hive-config.sh
export HADOOP_HOME=SOMEWHERE/hadoop-3.2.2                # Add these
export HIVE_HOME=SOMEWHERE/apache-hive-3.1.2-bin         # 3 lines to
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/      # end of file
```
It is very important that the java version is set to specifically Java 8. The newer Java versions (i.e. Java 11) are not compatible with a component in Hive called 'Spring Boot' and will cause you misery when you try to start the Hive Metastore service. 

Next, you need to ensure the Guava versions used in Hive and Hadoop match; if you installed Hive and Hadoop using the links above, they unfortunately will not. We'll fix this now:
```
$ cd SOMEWHERE/hadoop-3.2.2/share/hadoop/common/lib
$ cp guava-27.0-jre.jar SOMEWHERE/apache-hive-3.1.2-bin/lib
$ cd SOMEWHERE/apache-hive-3.1.2-bin/lib      # Don't forget to get rid of the old installation in Hive
$ rm guava-*older version*-jre.jar            # I got rid of mine already, so I can't remember the exact version. You can figure this out, right?
```

It is now time to connect Hive Metastore to the MySQL server which you started in the previous step. There are 3 config files which you will need to modify.
```
$ cd SOMEWHERE/apache-hive-3.1.2-bin/conf  
$ cp hive-env.sh.template hive-env.sh
$ vim hive-env.sh 
export HADOOP_HOME=SOMEWHERE/hadoop-3.2.2                     # Add these 2 lines
export HIVE_CONF_DIR=SOMEWHERE/apache-hive-3.1.2-bin/conf     # to the end of the file
```

The second file is in the same directory as the first; it tells Hive Metastore how it should connect to your MySQL server:
```
$ vim hive-site.xml
<configuration>
<property>
<name>javax.jdo.option.ConnectionURL</name>
<value>jdbc:mysql://localhost/hcatalog?createDatabaseIfNotExist=true&amp;serverTimezone=America/Chicago</value>
</property>
<property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>root</value>
</property>
<property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>YOUR_MYSQL_PASSWORD</value>
</property>
<property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.jdbc.Driver</value>
</property>
<property>
        <name>hive.metastore.warehouse.dir</name>
        <value>SOMEWHERE/warehouse/</value>   # Make sure that Hive has access to this directory
</property>
</configuration>
```
Copy the entire chunk into the file (if the file doesn't exist, create it yourself). note the ``serverTimezone`` on line 5; the specific value of ``serverTimezone`` you should use depends on the value of the ``system_time_zone`` variable of your MySQL server (since mine had ``system_time_zone = CDT``, I am using ``America/Chicago``). 
If the system time zone is ``UTC``, delete everything after ``true`` on line 5 - no need to set ``serverTimezone``.

The third file is in the hadoop directory:
```
$ cd SOMEWHERE/hadoop-3.2.2/etc/hadoop 
$ vim hadoop-env.sh
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/      # add this line to the end of the file
```

It is now time to start Hive Metastore:
```
$ ./hive --service metastore &
2022-04-07 23:53:26: Starting Hive Metastore Server
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/home/zl20/hive/apache-hive-3.1.2-bin/lib/log4j-slf4j-impl-2.10.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/home/zl20/hive/hadoop-3.2.2/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Loading class `com.mysql.jdbc.Driver'. This is deprecated. The new driver class is `com.mysql.cj.jdbc.Driver'. The driver is automatically registered via the SPI and manual loading of the driver class is generally unnecessary.
```
If you see all this, it means that Hive Metastore is up and running.

### Presto SQL server and client

Download the latest version of Presto server:
```
$ wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.270/presto-server-0.270.tar.gz
```

Unzip the server, go to the /lib directory and install the accompanying command line interface:
```
$ cd SOMEWHERE/presto-server-0.270/bin
$ wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.272/presto-cli-0.272-executable.jar
$ mv presto-cli-0.272-executable.jar presto
$ chmod +x presto ## make it executable
```
Unzip the command line interface in the same directory. 

Now we will set Presto configs; since this process was too tedious and error-prone, I used a premade set of configs found in a [very helpful Youtube video](https://www.youtube.com/watch?v=WAkLG3g0FOU)
```
$ cd SOMEWHERE/presto-server-0.270
$ wget http://media.sundog-soft.com/hadoop/presto-hdp-config.tgz
```

Unzip it. You should see a new directory ``etc`` pop up:
```
$ ls
ls
bin  etc  lib  NOTICE  plugin  presto-hdp-config.tgz  README.txt  sss
```

We will now set the data directory for Presto. This is where Presto dumps its logs.
```
$ cd etc
$ vim node.properties
node.data-dir=SOMEWHERE/presto_data   # Add this line if it doesn't exist; make sure Presto has access to this directory.
```

The rest of the configs, including the config for the Hive connector, should already be fully set up. We will now start the Presto server:
```
$ cd SOMEWHERE/presto-server-0.270/bin
$ ./launcher start
Started as 1758303
```

The server is now up. We can now connect to it (8090 is the default port if you downloaded the configs from the link above):
```
$ ./presto --server localhost:8090 --catalog hive
presto>
```
Congratulations! You have finished setting up the entire Presto cluster.
