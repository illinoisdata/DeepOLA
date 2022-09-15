select
	c_custkey,
	c_name,
	c_acctbal,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
from
	customer,
	orders,
	lineitem,
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1993-10-01' + interval '3' month
group by
	c_custkey,
	c_name,
	c_acctbal,
order by
	revenue desc;