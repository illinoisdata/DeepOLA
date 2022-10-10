SELECT sum(l_extendedprice * l_discount) as revenue
FROM
    lineitem
WHERE
    l_shipdate >= '1994-01-01' AND
    l_shipdate < '1995-01-01' AND
    l_discount >= 5 AND
    l_discount <= 7 AND
    l_quantity < 24
