\timing on
SELECT ONLINE SUM(l_extendedprice * (1 - l_discount))
FROM customer, lineitem, orders, nation
WHERE	c_custkey = o_custkey
	AND	l_orderkey = o_orderkey
	and o_orderdate >= date '1994-07-01'
	AND l_returnflag = 'R'
	AND c_nationkey = n_nationkey
	AND c_acctbal > 3000
WITHTIME 60000 CONFIDENCE 95 REPORTINTERVAL 1000;