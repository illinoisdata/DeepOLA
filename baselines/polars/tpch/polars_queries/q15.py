from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 15


def q():
    var_date = datetime(1996, 1, 1)
    var_date_interval_3mon = datetime(1996, 4, 1)

    supplier_ds = utils.get_supplier_ds()
    line_item_ds = utils.get_line_item_ds()

    final_cols = [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_phone",
        "total_revenue"
    ]

    revenue0 = (
        line_item_ds
        .filter((pl.col("l_shipdate") >= var_date) & (pl.col("l_shipdate") < var_date_interval_3mon))
        .with_column((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue"))
        .groupby("l_suppkey")
        .agg(pl.sum("revenue").alias('total_revenue'))
        .select(["l_suppkey", "total_revenue"])
        .rename({"l_suppkey": "supplier_no"})
    )

    max_total_revenue = revenue0.max().collect().get_column("total_revenue")

    q_final = (
        supplier_ds
        .join(revenue0.filter(pl.col("total_revenue") == max_total_revenue), left_on="s_suppkey", right_on="supplier_no")
        .select(final_cols)
        .sort("s_suppkey")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
