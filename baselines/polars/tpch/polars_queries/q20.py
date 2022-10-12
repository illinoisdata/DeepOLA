from datetime import datetime

import polars as pl

from polars_queries import utils

from linetimer import CodeTimer, linetimer

Q_NUM = 20


def q():
    VAR1 = datetime(1994, 1, 1)
    VAR2 = datetime(1995, 1, 1)

    line_item_ds = utils.get_line_item_ds()
    part_ds = utils.get_part_ds()
    part_supp_ds = utils.get_part_supp_ds()
    nation_ds = utils.get_nation_ds()
    supp_ds = utils.get_supplier_ds()

    part_subquery = (
        part_ds
        .filter(pl.col("p_name").str.starts_with("forest"))
        .select("p_partkey")
    )

    lineitem_group_ds = (
        line_item_ds
        .filter((pl.col("l_shipdate") >= VAR1) & (pl.col("l_shipdate") < VAR2))
        .groupby(["l_partkey","l_suppkey"])
        .agg((0.5 * pl.col("l_quantity").sum()).alias("l_quantity_sum"))
    )

    ps_suppkey_subquery = (
        part_supp_ds
        .join(part_subquery, left_on="ps_partkey", right_on="p_partkey")
        .join(lineitem_group_ds, left_on=["ps_partkey","ps_suppkey"], right_on=["l_partkey","l_suppkey"])
        .filter(pl.col("ps_availqty") > pl.col("l_quantity_sum").floor())
        .select("ps_suppkey")
    ).collect()

    q_final = (
        supp_ds
        .join(nation_ds.filter(pl.col("n_name") == "CANADA"), left_on="s_nationkey", right_on="n_nationkey")
        ## Alternate possibility: Instead of doing a collect(), perform a unique and then JOIN on ps_suppkey.
        # .join(ps_suppkey_subquery, left_on="s_suppkey", right_on="ps_suppkey")
        .filter(pl.col("s_suppkey").is_in(pl.lit(ps_suppkey_subquery.get_column("ps_suppkey"))))
        .select(["s_name","s_address"])
        .sort("s_name")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
