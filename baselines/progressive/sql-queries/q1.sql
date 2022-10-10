SELECT l_returnflag, l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (100 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (100 - l_discount) * (100 + l_tax)) as sum_charge,
    count(*) as count_order,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
FROM
    lineitem
WHERE
    l_shipdate <= '1998-09-02'
GROUP BY
    l_returnflag, l_linestatus
ORDER BY
    l_returnflag, l_linestatus
