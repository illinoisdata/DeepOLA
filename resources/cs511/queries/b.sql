select
	c_name,
	sum(o_totalprice) as o_totalprice_sum
from
	orders,
	customer
where
	o_custkey = c_custkey
	and c_mktsegment = 'AUTOMOBILE'
group by
	c_name
order by
	revenue desc