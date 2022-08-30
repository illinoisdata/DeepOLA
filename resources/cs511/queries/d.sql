select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
    p_partkey = l_partkey
    and l_shipinstruct = 'DELIVER IN PERSON'
    and
    (
        (
            p_brand = 'Brand#12'
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
        )
        or
        (
            p_brand = 'Brand#23'
            and l_quantity >= 10 and l_quantity <= 10 + 10
            and p_size between 1 and 10
        )
        or
        (
            p_brand = 'Brand#34'
            and l_quantity >= 20 and l_quantity <= 20 + 10
            and p_size between 1 and 15
        )
    )