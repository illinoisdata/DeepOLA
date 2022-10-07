from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 14


def q():
    VAR1 = datetime(1995, 9, 1)
    VAR2 = datetime(1995, 10, 1)

    line_item_ds = utils.get_line_item_ds()
    part_ds = utils.get_part_ds()

    q_final = (
        line_item_ds
        .filter((pl.col("l_shipdate") >= VAR1) & (pl.col("l_shipdate") < VAR2))
        .join(part_ds, left_on="l_partkey", right_on="p_partkey")
        .with_columns(
            [
                pl.col("p_type").str.starts_with("PROMO").alias("is_promo"),
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue"),
            ])
        .with_columns([
            (pl.col("revenue") * pl.col("is_promo")).alias("num_revenue"),
            pl.col("revenue").alias("den_revenue"),
        ])
        .sum()
        .select(
            (100.0 * pl.col("num_revenue")/pl.col("den_revenue")).alias("promo_revenue")
        )
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
