import os
from datetime import datetime
import polars as pl

from polars_queries import utils

#CORRECT OUTPUT
Q_NUM = 19

def q():
    line_item_ds = utils.get_line_item_ds()
    part_ds = utils.get_part_ds()

    cols = [
        "l_shipmode",
        "l_shipinstruct",
        "p_brand",
        "p_container",
        "l_quantity",
        "p_size"
    ]

    q_final = (
        line_item_ds
        .filter((pl.col("l_shipmode").is_in(pl.lit(pl.Series(["AIR","AIR REG"])))) & (pl.col("l_shipinstruct") == "DELIVER IN PERSON"))
        .join(part_ds, left_on="l_partkey", right_on="p_partkey")
        .filter(
            (
                (pl.col("p_brand") == "Brand#12") &
                (pl.col("p_container").is_in(pl.lit(pl.Series(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])))) &
                (pl.col("l_quantity").is_between(1,11,include_bounds=True)) &
                (pl.col("p_size").is_between(1,5,include_bounds=True)) &
                (pl.col("l_shipmode").is_in(pl.lit(pl.Series(["AIR","AIR REG"])))) &
                (pl.col("l_shipinstruct") == "DELIVER IN PERSON")
            ) |
            (
                (pl.col("p_brand") == "Brand#23") &
                (pl.col("p_container").is_in(pl.lit(pl.Series(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])))) &
                (pl.col("l_quantity").is_between(10,20,include_bounds=True)) &
                (pl.col("p_size").is_between(1,10,include_bounds=True))
            ) |
            (
                (pl.col("p_brand") == "Brand#34") &
                (pl.col("p_container").is_in(pl.lit(pl.Series(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])))) &
                (pl.col("l_quantity").is_between(20,30,include_bounds=True)) &
                (pl.col("p_size").is_between(1,15,include_bounds=True))
            )
        )
        .with_column(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        ).select("revenue").sum()
    )


    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()