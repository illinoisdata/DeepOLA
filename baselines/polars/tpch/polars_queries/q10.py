from datetime import datetime
# from turtle import left

import polars as pl

from polars_queries import utils

Q_NUM = 10


def q():
    var1 = datetime(1993, 10, 1)
    var2 = datetime(1994, 1, 1)
    var3 = "R"

    customer_ds = utils.get_customer_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    nation_ds = utils.get_nation_ds()

    q_final = (
        customer_ds.join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
        .filter(pl.col("o_orderdate") >= var1)
        .filter(pl.col("o_orderdate") < var2)
        .filter(pl.col("l_returnflag") == var3)
        .with_column(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .groupby(["c_custkey","c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"])
        .agg([pl.sum("revenue")])
        .select(
            [
                "c_custkey",
                "c_name",
                "revenue",
                "c_acctbal",
                "n_name",
                "c_address",
                "c_phone",
                "c_comment"
            ]
        )
        .sort(by="revenue", reverse=True)
        .with_column(pl.col(pl.datatypes.Utf8).str.strip().keep_name())
        .limit(20)
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()