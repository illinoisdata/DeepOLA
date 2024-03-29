from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 22


def q():
    var_list = [13, 31, 23, 29, 30, 18, 17]
    customer_ds = utils.get_customer_ds()
    orders_ds = utils.get_orders_ds()

    avg_c_acctbal = (
        customer_ds
        .filter((pl.col("c_acctbal") > 0) & (pl.col("c_phone").str.slice(0,2).cast(pl.Int32).is_in(pl.lit(pl.Series(var_list)))))
        .select(pl.col("c_acctbal"))
        .mean()
    ).collect()

    q_final = (
        customer_ds
        .with_column((pl.col("c_phone").str.slice(0,2).alias("cntrycode").cast(pl.Int32)))
        .filter(pl.col("cntrycode").is_in(pl.lit(pl.Series(var_list))))
        .filter(pl.col("c_acctbal") > pl.lit(avg_c_acctbal.get_column("c_acctbal")))
        .join(orders_ds, left_on=["c_custkey"], right_on=["o_custkey"],how="left")
        .filter(pl.col("o_orderkey").is_null())
        .groupby(["cntrycode"])
        .agg([
            pl.count().alias("numcust"),
            pl.col("c_acctbal").sum().alias("totacctbal")
        ]).sort("cntrycode")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()