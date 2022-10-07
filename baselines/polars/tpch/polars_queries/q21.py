from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 21


def q():
    var_nation = 'SAUDI ARABIA'

    supplier_ds = utils.get_supplier_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    nation_ds = utils.get_nation_ds()

    line_item_order_ds = (
        line_item_ds
        .join(orders_ds.filter(pl.col("o_orderstatus") == "F"), left_on="l_orderkey", right_on="o_orderkey")
    )

    line_item_order_faulty = (
        line_item_order_ds
        .filter(pl.col("l_receiptdate") > pl.col("l_commitdate"))
    )

    num_total_suppliers = (
        line_item_order_ds
        .groupby(["l_orderkey"])
        .agg([
            pl.col("l_suppkey").n_unique().alias("num_total_suppliers"),
        ])
        .filter(pl.col("num_total_suppliers") >= 2)
    )
    num_faulty_suppliers = (
        line_item_order_faulty
        .groupby(["l_orderkey"])
        .agg([
            pl.col("l_suppkey").n_unique().alias("num_faulty_supplier")
        ])
        .filter(pl.col("num_faulty_supplier") == 1)
    )

    q_final = (
        line_item_order_faulty
        .groupby(["l_orderkey","l_suppkey"])
        .agg(pl.count())
        .join(num_total_suppliers, left_on="l_orderkey", right_on="l_orderkey")
        .join(num_faulty_suppliers, left_on="l_orderkey", right_on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(nation_ds.filter(pl.col("n_name") == var_nation), left_on="s_nationkey", right_on="n_nationkey")
        .groupby("s_name")
        .agg(pl.count().alias("numwait"))
        .sort(["numwait", "s_name"], reverse=[True, False])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()