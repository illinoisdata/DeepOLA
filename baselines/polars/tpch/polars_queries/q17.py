from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 17


def q():
    lineitem_ds = utils.get_line_item_ds()
    part_ds = utils.get_part_ds()

    lineitem_grouped = (
        lineitem_ds
        .groupby(["l_partkey"])
        .agg(pl.col("l_quantity").mean().alias("l_quantity_avg"))
    )

    q_final = (
        part_ds
        .filter((pl.col("p_brand") == "Brand#23") & (pl.col("p_container") == "MED BOX"))
        .join(lineitem_ds, left_on="p_partkey", right_on="l_partkey")
        .join(lineitem_grouped, left_on="p_partkey", right_on="l_partkey")
        .filter(pl.col("l_quantity") < 0.2 * pl.col("l_quantity_avg"))
        .sum()
        .with_column((pl.col("l_extendedprice")/7.0).alias("avg_yearly"))
        .select(pl.col("avg_yearly"))
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
