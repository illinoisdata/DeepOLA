from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 2


def q():
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    region_ds = utils.get_region_ds()
    nation_ds = utils.get_nation_ds()
    supplier_ds = utils.get_supplier_ds()
    part_ds = utils.get_part_ds()
    part_supp_ds = utils.get_part_supp_ds()
    final_cols = [
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ]
    result_q1 = (
        part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("p_size") == var1)
        .filter(pl.col("p_type").str.ends_with(var2))
        .filter(pl.col("r_name") == var3)
    ).cache()

    result2 = (result_q1.groupby("p_partkey")
        .agg(pl.min("ps_supplycost").alias("ps_supplycost_min")))

    q_final = (
        result_q1.join(
            result2,
            left_on=["p_partkey", "ps_supplycost"],
            right_on=["p_partkey", "ps_supplycost_min"]
        )
        .select(final_cols)
        .sort(
            by=["s_acctbal", "n_name", "s_name", "p_partkey"],
            reverse=[True, False, False, False],
        )
        .limit(100)
        .with_column(pl.col(pl.datatypes.Utf8).str.strip().keep_name())
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()