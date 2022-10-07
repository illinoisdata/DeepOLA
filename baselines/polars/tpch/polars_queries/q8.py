from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 8


def q():
    nation_ds = utils.get_nation_ds()
    customer_ds = utils.get_customer_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    supplier_ds = utils.get_supplier_ds()
    part_ds = utils.get_part_ds()
    region_ds = utils.get_region_ds()

    # Query Variables
    var_date_start = datetime(1995,1,1)
    var_date_end = datetime(1996,12,31)
    var_r_name = 'AMERICA'
    var_s_nation = 'BRAZIL'
    var_p_type = 'ECONOMY ANODIZED STEEL'

    q_final = (
        line_item_ds
        .join(
            orders_ds.filter((pl.col("o_orderdate") >= var_date_start) & (pl.col("o_orderdate") <= var_date_end)),
            left_on = "l_orderkey",
            right_on = "o_orderkey"
        )
        .join(
            part_ds.filter(pl.col("p_type") == var_p_type),
            left_on = "l_partkey",
            right_on = "p_partkey"
        )
        .join(
            customer_ds,
            left_on = "o_custkey",
            right_on = "c_custkey"
        )
        .join(
            nation_ds.join(region_ds.filter(pl.col("r_name") == var_r_name), left_on = "n_regionkey", right_on = "r_regionkey"),
            left_on = "c_nationkey",
            right_on = "n_nationkey"
        )
        .join(
            supplier_ds.join(nation_ds, left_on = "s_nationkey", right_on = "n_nationkey").rename({"n_name": "n2.n_name"}),
            left_on = "l_suppkey",
            right_on = "s_suppkey"
        )
        .with_columns([
            pl.col("o_orderdate").dt.year().alias("o_year"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume"),
            pl.col("n2.n_name").alias("nation")
        ])
        .groupby(["o_year"])
        .agg([
            ((pl.col("volume") * (pl.col("n2.n_name") == var_s_nation)).sum()/pl.col("volume").sum()).alias("mkt_share")
        ])
        .sort(["o_year"])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()