CREATE EXTERNAL TABLE $schema.customer (
	c_custkey     BIGINT,
	c_name        VARCHAR(25), 
	c_address     VARCHAR(40),
	c_nationkey   BIGINT,
	c_phone       CHAR(15),
	c_acctbal     DOUBLE,
	c_mktsegment  CHAR(10),
	c_comment     VARCHAR(117),
	c_dummy       VARCHAR(10),
	PRIMARY KEY (c_custkey) disable novalidate)
STORED AS PARQUET;


-- nation
CREATE EXTERNAL TABLE $schema.nation (
	n_nationkey  BIGINT,
	n_name       CHAR(25),
	n_regionkey  BIGINT,
	n_comment    VARCHAR(152),
	n_dummy      VARCHAR(10),
	PRIMARY KEY (n_nationkey) disable novalidate)
STORED AS PARQUET;


-- region
CREATE EXTERNAL TABLE $schema.region (
	r_regionkey  BIGINT,
	r_name       CHAR(25),
	r_comment    VARCHAR(152),
	r_dummy      VARCHAR(10),
	PRIMARY KEY (r_regionkey) disable novalidate)
STORED AS PARQUET;



-- supplier
CREATE EXTERNAL TABLE $schema.supplier (
	s_suppkey     BIGINT,
	s_name        CHAR(25),
	s_address     VARCHAR(40),
	s_nationkey   BIGINT,
	s_phone       CHAR(15),
	s_acctbal     DOUBLE,
	s_comment     VARCHAR(101),
	s_dummy varchar(10),
	PRIMARY KEY (s_suppkey) disable novalidate)
STORED AS PARQUET;



-- part
CREATE EXTERNAL TABLE $schema.part (
	p_partkey     BIGINT,
	p_name        VARCHAR(55),
	p_mfgr        CHAR(25),
	p_brand       CHAR(10),
	p_type        VARCHAR(25),
	p_size        BIGINT,
	p_container   CHAR(10),
	p_retailprice DOUBLE ,
	p_comment     VARCHAR(23) ,
	p_dummy       VARCHAR(10),
	PRIMARY KEY (p_partkey) disable novalidate)
STORED AS PARQUET;



-- partsupp
CREATE EXTERNAL TABLE $schema.partsupp (
	ps_partkey     BIGINT,
	ps_suppkey     BIGINT,
	ps_availqty    BIGINT,
	ps_supplycost  DOUBLE,
	ps_comment     VARCHAR(199),
	ps_dummy       VARCHAR(10),
	PRIMARY KEY (ps_partkey) disable novalidate)
STORED AS PARQUET;



-- orders
CREATE EXTERNAL TABLE $schema.orders (
	o_orderkey       BIGINT,
	o_custkey        BIGINT,
	o_orderstatus    CHAR(1),
	o_totalprice     DOUBLE,
	o_orderdate      DATE,
	o_orderpriority  CHAR(15),
	o_clerk          CHAR(15),
	o_shippriority   BIGINT,
	o_comment        VARCHAR(79),
	o_dummy          VARCHAR(10),
	PRIMARY KEY (o_orderkey) disable novalidate)
STORED AS PARQUET;



-- lineitem
CREATE EXTERNAL TABLE $schema.lineitem (
	l_orderkey    BIGINT,
	l_partkey     BIGINT,
	l_suppkey     BIGINT,
	l_linenumber  BIGINT,
	l_quantity    BIGINT,
	l_extendedprice  DOUBLE,
	l_discount    DOUBLE,
	l_tax         DOUBLE,
	l_returnflag  CHAR(1),
	l_linestatus  CHAR(1),
	l_shipdate    DATE,
	l_commitdate  DATE,
	l_receiptdate DATE,
	l_shipinstruct CHAR(25),
	l_shipmode    CHAR(10),
	l_comment     VARCHAR(44),
	l_dummy       VARCHAR(10))
STORED AS PARQUET;
