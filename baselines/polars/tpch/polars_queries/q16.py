import os
from datetime import datetime
import polars as pl

from polars_queries import utils

#CORRECT OUTPUT
Q_NUM = 16

def q():
    part_ds = utils.get_part_ds()
    part_supp_ds = utils.get_part_supp_ds()
    supp_ds = utils.get_supplier_ds()

    supp_ds_filter = (
        supp_ds
        .filter(pl.col("s_comment").str.contains("(.*)Customer(.*)Complaints(.*)"))
        .select("s_suppkey")
    ).collect()

    part_ds_filter = (
        part_ds
        .filter((pl.col("p_brand") != "Brand#45") &
            (~pl.col("p_type").str.starts_with("MEDIUM POLISHED")) &
            (pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        )
    )

    q_final = (
        part_supp_ds
        .filter(~pl.col("ps_suppkey").is_in(pl.lit(supp_ds_filter.get_column("s_suppkey"))))
        .join(part_ds_filter, left_on="ps_partkey", right_on="p_partkey")
        .groupby(["p_brand","p_type","p_size"])
        .agg(pl.col("ps_suppkey").n_unique().alias("supplier_cnt"))
        .sort(["supplier_cnt","p_brand","p_type","p_size"], reverse=[True,False,False,False])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
