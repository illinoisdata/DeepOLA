-- WanderJoin Query7
SELECT sum(l_extendedprice * (1 - l_discount))
FROM nation n1, supplier, lineitem, orders, customer, nation n2
WHERE s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND n1.n_name = 'CHINA'
    AND l_shipdate > date '1994-05-01';