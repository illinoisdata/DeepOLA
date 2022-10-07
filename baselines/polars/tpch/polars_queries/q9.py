from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 9

def q():
    var_color = 'green' #LIKE GREEN

    part_ds = utils.get_part_ds()
    supplier_ds = utils.get_supplier_ds()
    line_item_ds = utils.get_line_item_ds()
    part_supp_ds = utils.get_part_supp_ds()
    orders_ds = utils.get_orders_ds()
    nation_ds = utils.get_nation_ds()

    final_cols = [
        "nation",
        "o_year",
        "sum_profit",
    ]
    q_final = (
        line_item_ds.join(part_supp_ds, left_on=["l_suppkey", "l_partkey"], right_on=["ps_suppkey", "ps_partkey"])
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .join(part_ds, left_on="l_partkey", right_on="p_partkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("p_name").str.contains(var_color))
        .rename({"n_name": "nation"})
        .with_column(pl.col("o_orderdate").dt.year().alias("o_year")) #extract year from orderdate
        .with_column(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")) - pl.col("ps_supplycost") * pl.col("l_quantity")).alias("amount")
            )
        .groupby(["nation", "o_year"])
        .agg([pl.sum("amount").alias("sum_profit")])
        .select(final_cols)
        .sort(by=["nation", "o_year"], reverse=[False, True])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()