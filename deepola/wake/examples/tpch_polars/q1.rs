use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
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
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("l_shipdate").unwrap();
            let mask = a.lt_eq("1998-09-01").unwrap();
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
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator.set_group_key(vec!["l_returnflag".to_string(), "l_linestatus".to_string()]);
    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&groupby_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(lineitem_csvreader_node);
    service
}
