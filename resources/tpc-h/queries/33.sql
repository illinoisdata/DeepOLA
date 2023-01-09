SELECT 
    R_NAME, 
    N_NAME,
    AVG(L_QUANTITY)
FROM
    (
    SELECT 
        R_NAME,
        N_NAME,
        L_SHIPMODE,
        AVG(L_QUANTITY) AS L_QUANTITY
    FROM
        (
        SELECT
            R_NAME,
            N_NAME,
            L_SHIPMODE,
            O_SHIPPRIORITY,
            L_LINESTATUS,
            L_RETURNFLAG,
            AVG(L_QUANTITY) AS L_QUANTITY
        FROM 
            (
                SELECT
                    R_NAME,
                    N_NAME,
                    L_SHIPMODE,
                    O_ORDERPRIORITY,
                    O_SHIPPRIORITY,
                    L_LINESTATUS,
                    L_RETURNFLAG,
                    AVG(L_QUANTITY) AS L_QUANTITY
                FROM
                    LINEITEM, SUPPLIER, NATION, REGION, ORDERS
                WHERE
                    L_SUPPKEY = S_SUPPKEY AND
                    L_ORDERKEY = O_ORDERKEY AND
                    S_NATIONKEY = N_NATIONKEY AND
                    N_REGIONKEY = R_REGIONKEY AND 
                    L_SHIPDATE >= '1996-01-01' AND 
                    L_SHIPDATE < '1997-01-01'
                GROUP BY
                    R_NAME,
                    N_NAME,
                    L_SHIPMODE,
                    O_ORDERPRIORITY,
                    O_SHIPPRIORITY,
                    L_LINESTATUS,
                    L_RETURNFLAG
            ) T1
        GROUP BY
            R_NAME,
            N_NAME,
            L_SHIPMODE,
            O_SHIPPRIORITY,
            L_LINESTATUS,
            L_RETURNFLAG
        ) T2
    GROUP BY
        R_NAME,
        N_NAME,
        L_SHIPMODE
    ) T3
GROUP BY
    R_NAME,
    N_NAME;