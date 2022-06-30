<p>Before using Presto make sure to start up Hive Metastore with the following command:</p>

```
hive --service metastore &
```

To create the data do the following:
- Download dbgen by running `git clone https://github.com/electrum/tpch-dbgen.git` in the terminal
- edit the makefile.suite with the following values:

```
CC=gcc
DATABASE=INFORMIX
MACHINE=LINUX
WORKLOAD=TPCH
```

- Run `make -f makefile.suite`
- Go to the directory you want to make the data in and copy the `dbgen` and `dists.dss` file into this directory
- Run `./dbgen -s NUMBER` where NUMBER is just the size of the dataset. For example, for 10gb run `./dbgen -s 10`


<p>First create a directory in HDFS called tpch_data. To load the data into HDFS run the following commands in the terminal</p>

```
hdfs dfs -put customer.tbl /tpch_data
hdfs dfs -put lineitem.tbl /tpch_data
hdfs dfs -put nation.tbl /tpch_data
hdfs dfs -put orders.tbl /tpch_data
hdfs dfs -put part.tbl /tpch_data
hdfs dfs -put partsupp.tbl /tpch_data
hdfs dfs -put region.tbl /tpch_data
hdfs dfs -put supplier.tbl /tpch_data
```

<p>To set up the Database on HDFS, I used the following DDL Commands to create the tables in Hive </p>

```
CREATE EXTERNAL TABLE CUSTOMER (
c_custkey     INT,
c_name        VARCHAR(25), 
c_address     VARCHAR(40),
c_nationkey   INT,
c_phone       CHAR(15),
c_acctbal     DECIMAL(15,2),
c_mktsegment  CHAR(10),
c_comment     VARCHAR(117),
c_dummy       VARCHAR(10),
PRIMARY KEY (c_custkey) disable novalidate)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';


-- nation
CREATE EXTERNAL TABLE nation (
n_nationkey  INT,
n_name       CHAR(25),
n_regionkey  INT,
n_comment    VARCHAR(152),
n_dummy      VARCHAR(10),
PRIMARY KEY (n_nationkey) disable novalidate)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';


-- region
CREATE EXTERNAL TABLE region (
r_regionkey  INT,
r_name       CHAR(25),
r_comment    VARCHAR(152),
r_dummy      VARCHAR(10),
PRIMARY KEY (r_regionkey) disable novalidate)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';



-- supplier
CREATE EXTERNAL TABLE supplier (
s_suppkey     INT,
s_name        CHAR(25),
s_address     VARCHAR(40),
s_nationkey   INT,
s_phone       CHAR(15),
s_acctbal     DECIMAL(15,2),
s_comment     VARCHAR(101),
s_dummy varchar(10),
PRIMARY KEY (s_suppkey) disable novalidate)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';



-- part
CREATE EXTERNAL TABLE part (
p_partkey     INT,
p_name        VARCHAR(55),
p_mfgr        CHAR(25),
p_brand       CHAR(10),
p_type        VARCHAR(25),
p_size        INT,
p_container   CHAR(10),
p_retailprice DECIMAL(15,2) ,
p_comment     VARCHAR(23) ,
p_dummy       VARCHAR(10),
PRIMARY KEY (p_partkey) disable novalidate)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';



-- partsupp
CREATE EXTERNAL TABLE partsupp (
ps_partkey     INT,
ps_suppkey     INT,
ps_availqty    INT,
ps_supplycost  DECIMAL(15,2),
ps_comment     VARCHAR(199),
ps_dummy       VARCHAR(10),
PRIMARY KEY (ps_partkey) disable novalidate)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';



-- orders
CREATE EXTERNAL TABLE orders (
o_orderkey       INT,
o_custkey        INT,
o_orderstatus    CHAR(1),
o_totalprice     DECIMAL(15,2),
o_orderdate      DATE,
o_orderpriority  CHAR(15),
o_clerk          CHAR(15),
o_shippriority   INT,
o_comment        VARCHAR(79),
o_dummy          VARCHAR(10),
PRIMARY KEY (o_orderkey) disable novalidate)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';



-- lineitem
CREATE EXTERNAL TABLE lineitem (
l_orderkey    INT,
l_partkey     INT,
l_suppkey     INT,
l_linenumber  INT,
l_quantity    DECIMAL(15,2),
l_extendedprice  DECIMAL(15,2),
l_discount    DECIMAL(15,2),
l_tax         DECIMAL(15,2),
l_returnflag  CHAR(1),
l_linestatus  CHAR(1),
l_shipdate    DATE,
l_commitdate  DATE,
l_receiptdate DATE,
l_shipinstruct CHAR(25),
l_shipmode    CHAR(10),
l_comment     VARCHAR(44),
l_dummy       VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
```
<p> Then to load the tables with the data in HDFS, run the following commands </p>

```
LOAD DATA INPATH '/tpch_data/customer.tbl' INTO TABLE CUSTOMER;
LOAD DATA INPATH '/tpch_data/nation.tbl' INTO TABLE nation;
LOAD DATA INPATH '/tpch_data/region.tbl' INTO TABLE region;
LOAD DATA INPATH '/tpch_data/supplier.tbl' INTO TABLE supplier;
LOAD DATA INPATH '/tpch_data/part.tbl' INTO TABLE part;
LOAD DATA INPATH '/tpch_data/partsupp.tbl' INTO TABLE partsupp;
LOAD DATA INPATH '/tpch_data/orders.tbl' INTO TABLE orders;
LOAD DATA INPATH '/tpch_data/lineitem.tbl' INTO TABLE lineitem;
```
