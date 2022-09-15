\timing on
SELECT ONLINE SUM(l_extendedprice * (1 - l_discount))
FROM customer, orders, lineitem
WHERE   c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < date '1992-03-15'
    and l_shipdate > date '1990-03-15'
WITHTIME 60000 CONFIDENCE 95 REPORTINTERVAL 1000;



