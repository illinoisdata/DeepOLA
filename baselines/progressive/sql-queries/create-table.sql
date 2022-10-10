CREATE TABLE IF NOT EXISTS lineitem (
	l_orderkey int,
	l_partkey int,
	l_suppkey int,
	l_linenumber int,
	l_quantity int,
	l_extendedprice int,
	l_discount int,
	l_tax int,
	l_returnflag VARCHAR(1),
	l_linestatus VARCHAR(1),
	l_shipdate VARCHAR(44),
	l_commitdate VARCHAR(44),
	l_receiptdate VARCHAR(44),
	l_shipinstruct VARCHAR(44),
	l_shipmode VARCHAR(44),
	l_comment VARCHAR(44)
)
