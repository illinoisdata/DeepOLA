<h2>This guide details setting up Wanderjoin postgres installation</h2>



<h2> Run the following commands to install prequisite software packages</h2>

```
sudo apt-get update -y 
sudo apt install build-essential
sudo apt install bison
sudo apt-get install -y m4 
```

<h2>There is an issue with the Flex installation so you must install version 2.5.31 exactly. To do this, do the following:</h2>

- run `wget https://launchpad.net/ubuntu/+archive/primary/+sourcefiles/flex/2.5.31-31/flex_2.5.31.orig.tar.gz`
- extract the files from the downloaded package
- enter the directory you extracted the files into
- run `PATH=$PATH:/usr/local/m4/bin/`
- run `./configure --prefix=/usr/local/flex`
- run `sudo make`
- run `sudo make install`
- run `PATH=$PATH:/usr/local/flex/bin`
- if done correctly, running `flex --version` should say 2.5.31


<h2>To set up postgres, run the following commands:</h2>

```
./configure --without-realine
make
sudo su
make install
adduser postgres
mkdir /usr/local/pgsql/data
chown postgres /usr/local/pgsql/data
su - postgres
/usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data
/usr/local/pgsql/bin/postgres -D /usr/local/pgsql/data >logfile 2>&1 &
/usr/local/pgsql/bin/createdb test
/usr/local/pgsql/bin/psql test
```

<h2>You should be in the psql terminal now. The tables given in the wanderjoin documenation do not work with the tpch dbgen data, use the following
following commands to create the tables instead:</h2>

```
CREATE TABLE region (
	r_regionkey INTEGER PRIMARY KEY,
	r_name CHAR(25),
	r_comment VARCHAR(152),
	r_dummy   VARCHAR(10)
);

CREATE TABLE nation (
	n_nationkey INTEGER PRIMARY KEY,
	n_name CHAR(25), 
	n_regionkey INTEGER REFERENCES region(r_regionkey),
	n_comment VARCHAR(152),
	n_dummy   VARCHAR(10)
);

CREATE TABLE supplier (
	s_suppkey INTEGER PRIMARY KEY,
	s_name CHAR(25),
	s_address VARCHAR(40),
	s_nationkey INTEGER REFERENCES nation(n_nationkey),
	s_phone CHAR(15),
	s_acctbal DECIMAL(12, 2),
	s_comment VARCHAR(101),
	s_dummy   VARCHAR(10)
);

CREATE TABLE part (
	p_partkey INTEGER PRIMARY KEY,
	p_name VARCHAR(55),
	p_mfgr CHAR(25),
	p_brand CHAR(10),
	p_type VARCHAR(25),
	p_size INTEGER,
	p_container CHAR(10),
	p_retailprice DECIMAL(12, 2),
	p_comment VARCHAR(23),
	p_dummy   VARCHAR(10)
);

CREATE TABLE customer (
	c_custkey INTEGER PRIMARY KEY,
	c_name VARCHAR(25),
	c_address VARCHAR(40),
	c_nationkey INTEGER REFERENCES nation(n_nationkey),
	c_phone CHAR(15),
	c_acctbal DECIMAL(12, 2),
	c_mktsegment CHAR(10),
	c_comment VARCHAR(117),
  	c_dummy   VARCHAR(10)

);

CREATE TABLE partsupp (
	ps_partkey INTEGER REFERENCES part(p_partkey),
	ps_suppkey INTEGER REFERENCES supplier(s_suppkey),
	ps_availqty INTEGER,
	ps_supplycost DECIMAL(12, 2),
	ps_comment VARCHAR(199),
	ps_dummy   VARCHAR(10),
	PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE orders (
	o_orderkey INTEGER PRIMARY KEY,
	o_custkey INTEGER REFERENCES customer(c_custkey),
	o_orderstatus CHAR(1),
	o_totalprice DECIMAL(12, 2),
	o_orderdate DATE,
	o_orderpriority CHAR(15),
	o_clerk CHAR(15),
	o_shippriority INTEGER,
	o_comment VARCHAR(79),
	o_dummy   VARCHAR(10)
);

CREATE TABLE lineitem (
	l_orderkey INTEGER REFERENCES orders(o_orderkey),
	l_partkey INTEGER,
	l_suppkey INTEGER,
	l_linenumber INTEGER,
	l_quantity DECIMAL(12, 2),
	l_extendedprice DECIMAL(12, 2),
	l_discount DECIMAL(12, 2),
	l_tax DECIMAL(12, 2),
	l_returnflag CHAR(1),
	l_linestatus CHAR(1),
	l_shipdate DATE,
	l_commitdate DATE,
	l_receiptdate DATE,
	l_shipinstruct CHAR(25),
	l_shipmode CHAR(10),
	l_comment VARCHAR(44),
	l_dummy   VARCHAR(10),
	PRIMARY KEY (l_orderkey, l_linenumber),
	FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey)
);
```
<h2> Then to load the tables with the data in HDFS, run the following commands (file path should be where you stored the data)</h2>

```
\copy "region"     from '$FILE_PATH/region.tbl'        DELIMITER '|' CSV;
\copy "nation"     from '$FILE_PATH/nation.tbl'        DELIMITER '|' CSV;
\copy "customer"   from '$FILE_PATH/customer.tbl'    DELIMITER '|' CSV;
\copy "supplier"   from '$FILE_PATH/supplier.tbl'    DELIMITER '|' CSV;
\copy "part"       from '$FILE_PATH/part.tbl'            DELIMITER '|' CSV;
\copy "partsupp"   from '$FILE_PATH/partsupp.tbl'    DELIMITER '|' CSV;
\copy "orders"     from '$FILE_PATH/orders.tbl'        DELIMITER '|' CSV;
\copy "lineitem"   from '$FILE_PATH/lineitem.tbl'    DELIMITER '|' CSV;
```

<h2>The indexing commands from the documentation also have errors, run the following commands instead:</h2>

```
create index on customer using btree (c_mktsegment);
create index on orders using btree (o_custkey);
create index on lineitem using btree (l_orderkey);
create index on lineitem using btree (l_returnflag);
create index on nation using btree (n_name);
create index on supplier using btree (s_nationkey);
create index on customer using btree (c_nationkey);
create index on lineitem using btree (l_suppkey);
create index on lineitem using btree (l_shipdate);
```
<h2> Run the following commands as well:</h2>

```
\timing on
scan customer;
scan lineitem;
scan nation;
scan orders;
scan part;
scan partsupp;
scan region;
scan supplier;
scan customer_c_mktsegment_idx;
scan customer_c_nationkey_idx;
scan customer_pkey;
scan lineitem_l_orderkey_idx;
scan lineitem_l_orderkey_idx1;
scan lineitem_l_returnflag_idx;
scan lineitem_l_shipdate_idx;
scan lineitem_l_suppkey_idx;
scan lineitem_pkey;
scan nation_n_name_idx;
scan nation_n_regionkey_idx;
scan nation_pkey;
scan orders_o_custkey_idx;
scan orders_o_orderdate_idx;
scan orders_pkey;
scan part_pkey;
scan partsupp_pkey;
scan partsupp_ps_suppkey_idx;
scan region_pkey;
scan region_r_name_idx;
scan supplier_pkey;
scan supplier_s_nationkey_idx;
```

<h2> The tables should be setup and work now with wanderjoin queries</h2>
