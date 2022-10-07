from datetime import datetime

import polars as pl

from polars_queries import utils

Q_NUM = 12


def q():
    var_ship_mode1 = 'MAIL'
    var_ship_mode2 = 'SHIP'
    var_date = datetime(1994, 1, 1)
    var_date_interval_1yr = datetime(1995, 1, 1)

    line_item_ds = utils.get_line_item_ds()
    orders_ds = utils.get_orders_ds()

    q_final = (
        orders_ds.join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter((pl.col("l_shipmode") == var_ship_mode1) | (pl.col("l_shipmode") == var_ship_mode2))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .filter(pl.col("l_shipdate") < pl.col("l_commitdate"))
        .filter(pl.col("l_receiptdate") >= var_date)
        .filter(pl.col("l_receiptdate") < var_date_interval_1yr)
        .groupby(["l_shipmode"])
        .agg(
            [
                ((pl.col("o_orderpriority") == "1-URGENT") | (pl.col("o_orderpriority") == "2-HIGH")).sum().alias("high_line_count"),
                ((pl.col("o_orderpriority") != "1-URGENT") & (pl.col("o_orderpriority") != "2-HIGH")).sum().alias("low_line_count")
            ]
        )
        .sort(by="l_shipmode")
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()