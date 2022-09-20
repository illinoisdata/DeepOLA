import os

import polars as pl

from polars_queries import utils

Q_NUM = 21

#INCOMPLETE

def q():
    var_nation = 'SAUDI ARABIA'

    supplier_ds = utils.get_supplier_ds()
    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()
    nation_ds = utils.get_nation_ds()

    final_cols = [
    ]

    def result_q1(l1_l_orderkey, l1_l_suppkey) -> pl.Expr:
        print(type(l1_l_orderkey))
        r_count = len((line_item_ds
                    .filter((pl.col("l_orderkey") == l1_l_orderkey)
                        & (pl.col("l_suppkey") != l1_l_suppkey))).collect())
        print('r_count', r_count)
        if r_count == 0:
            return True
        return False
    def result_q2(l1_l_orderkey, l1_l_suppkey) -> pl.Expr:
        r_count = len((line_item_ds
                    .filter((pl.col("l_orderkey") == l1_l_orderkey)
                     & (pl.col("l_suppkey") != l1_l_suppkey)
                     & (pl.col("l_receiptdate") > pl.col("l_commitdate")))).collect())
        if r_count == 0:
            return True
        return False

    q_final = (
        line_item_ds.join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("o_orderstatus")=="F")
        .filter(pl.col("l_receiptdate") > pl.col("l_commitdate"))
        #.filter(result_q1(pl.col("l_orderkey"), pl.col("l_suppkey")))
        #.where(result_q2(pl.col("l_orderkey"), pl.col("l_suppkey")))
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("n_name") == var_nation)
        .groupby("s_name")
        .agg(pl.count("n_name").alias("numwait")) #any column
        .sort(["numwait", "s_name"], reverse=[True, False])
        .limit(100)
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()