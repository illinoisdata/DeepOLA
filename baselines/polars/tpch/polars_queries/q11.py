from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 11


def q():
    var_n_name = "GERMANY"

    partsupp_ds = utils.get_part_supp_ds()
    supplier_ds = utils.get_supplier_ds()
    nation_ds = utils.get_nation_ds()

    ps_supplycost_agg = (
        partsupp_ds
        .join(
            supplier_ds.join(
                nation_ds.filter(pl.col("n_name") == var_n_name),
                left_on = "s_nationkey",
                right_on = "n_nationkey"
            ),
            left_on = "ps_suppkey",
            right_on = "s_suppkey"
        )
        .with_column((pl.col("ps_supplycost") * pl.col("ps_availqty") * 0.0001).alias("value_limit"))
        .select(["value_limit"])
        .sum()
    ).collect()

    q_final = (
        partsupp_ds
        .join(
            supplier_ds.join(
                nation_ds.filter(pl.col("n_name") == var_n_name),
                left_on = "s_nationkey",
                right_on = "n_nationkey"
            ),
            left_on = "ps_suppkey",
            right_on = "s_suppkey"
        )
        .groupby(["ps_partkey"])
        .agg([
            (pl.col("ps_supplycost") * pl.col("ps_availqty")).sum().alias("value")
        ])
        .filter(pl.col("value") > pl.lit(ps_supplycost_agg.get_column("value_limit")))
        .sort(["value"], reverse=[True])
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
