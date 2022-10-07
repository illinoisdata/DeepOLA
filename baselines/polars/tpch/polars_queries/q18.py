from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 18


def q():
    var_quantity = 300

    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    customer_ds = utils.get_customer_ds()

    final_cols = [
        "c_name",
        "c_custkey",
        "o_orderkey",
        "o_orderdate",
        "o_totalprice",
        "sum"
    ]
    filtered_line_item_ds = (line_item_ds.groupby("l_orderkey")
        .agg(pl.sum("l_quantity").alias("sum_l_quantity"))
        .select(["l_orderkey", "sum_l_quantity"])
        .filter(pl.col("sum_l_quantity") > var_quantity)
        .with_column(pl.col("sum_l_quantity").cast(pl.datatypes.Float64).alias("sum")))

    q_final = (
        customer_ds.join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(filtered_line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .select(final_cols)
        .sort(["o_totalprice", "o_orderdate"], reverse=[True, False])
    )


    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()