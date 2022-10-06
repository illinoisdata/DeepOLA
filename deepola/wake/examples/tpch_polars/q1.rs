use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::inference::AggregateScaler;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query
// select
// 	l_returnflag,
// 	l_linestatus,
// 	sum(l_quantity) as sum_qty,
// 	sum(l_extendedprice) as sum_base_price,
// 	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
// 	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
// 	avg(l_quantity) as avg_qty,
// 	avg(l_extendedprice) as avg_price,
// 	avg(l_discount) as avg_disc,
// 	count(*) as count_order
// from
// 	lineitem
// where
// 	l_shipdate <= date '1998-12-01' - interval '90' day
// group by
// 	l_returnflag,
// 	l_linestatus
// order by
// 	l_returnflag,
// 	l_linestatus;
// limit -1;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([(
        "lineitem".into(),
        vec![
            "l_orderkey", // For COUNT(*) counting a key.
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
        ],
    )]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("l_shipdate").unwrap();
            let mask = a.lt_eq("1998-09-02").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let tax = df.column("l_tax").unwrap();
            let columns = vec![
                Series::new(
                    "disc_price",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64),
                ),
                Series::new(
                    "charge",
                    (extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64))
                        * (tax + 1f64),
                ),
            ];
            df.hstack(&columns).unwrap()
        })))
        .build();

    // GROUP BY Aggregate Node
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["l_returnflag".to_string(), "l_linestatus".to_string()])
        .set_aggregates(vec![
            ("l_orderkey".into(), vec!["count".into()]),
            ("l_quantity".into(), vec!["sum".into()]),
            ("l_extendedprice".into(), vec!["sum".into()]),
            ("l_discount".into(), vec!["sum".into()]),
            ("disc_price".into(), vec!["sum".into()]),
            ("charge".into(), vec!["sum".into()]),
        ]);

    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    let scaler_node = AggregateScaler::new_growing()
        .count_column("l_orderkey_count".into())
        .scale_count("l_orderkey_count".into())
        .scale_sum("l_quantity_sum".into())
        .scale_sum("l_extendedprice_sum".into())
        .scale_sum("l_discount_sum".into())
        .scale_sum("disc_price_sum".into())
        .scale_sum("charge_sum".into())
        .into_node();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // Compute AVG from SUM/COUNT.
            let columns = vec![
                Series::new("l_returnflag", df.column("l_returnflag").unwrap()),
                Series::new("l_linestatus", df.column("l_linestatus").unwrap()),
                Series::new("sum_qty", df.column("l_quantity_sum").unwrap()),
                Series::new("sum_base_price", df.column("l_extendedprice_sum").unwrap()),
                Series::new("sum_disc_price", df.column("disc_price_sum").unwrap()),
                Series::new("sum_charge", df.column("charge_sum").unwrap()),
                Series::new(
                    "avg_qty",
                    (df.column("l_quantity_sum")
                        .unwrap()
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap())
                        / (df
                            .column("l_orderkey_count")
                            .unwrap()
                            .cast(&polars::datatypes::DataType::Float64)
                            .unwrap()),
                ),
                Series::new(
                    "avg_price",
                    df.column("l_extendedprice_sum").unwrap()
                        / df.column("l_orderkey_count").unwrap(),
                ),
                Series::new(
                    "avg_disc",
                    df.column("l_discount_sum").unwrap() / df.column("l_orderkey_count").unwrap(),
                ),
                Series::new("count_order", df.column("l_orderkey_count").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["l_returnflag", "l_linestatus"], vec![false, false])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    scaler_node.subscribe_to_node(&groupby_node, 0);
    select_node.subscribe_to_node(&scaler_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(select_node);
    service.add(scaler_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(lineitem_csvreader_node);
    service
}
