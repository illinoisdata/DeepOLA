from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 13


def q():
    orders_ds = utils.get_orders_ds()
    customer_ds = utils.get_customer_ds()

    q_final = (
        customer_ds
        .join(
            orders_ds.filter(~pl.col("o_comment").str.contains("(.*)special(.*)requests(.*)")),
            left_on = "c_custkey",
            right_on = "o_custkey",
            how = "left"
        )
        .groupby(["c_custkey"])
        .agg(pl.col("o_orderkey").drop_nulls().count().alias("c_count"))
        .groupby(["c_count"])
        .agg(pl.count().alias("custdist"))
        .sort(["custdist","c_count"], reverse=[True, True])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()