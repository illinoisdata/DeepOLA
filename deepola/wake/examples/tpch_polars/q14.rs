use crate::utils::*;

extern crate wake;
use itertools::Itertools;
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
// 	100.00 * sum(case
// 		when p_type like 'PROMO%'
// 			then l_extendedprice * (1 - l_discount)
// 		else 0
// 	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
// from
// 	lineitem,
// 	part
// where
// 	l_partkey = p_partkey
// 	and l_shipdate >= date '1995-09-01'
// 	and l_shipdate < date '1995-09-01' + interval '1' month;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_partkey", "l_extendedprice", "l_discount", "l_shipdate"],
        ),
        ("part".into(), vec!["p_partkey", "p_type"]),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_reader_node("part".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("l_shipdate").unwrap();
            let mask = a.gt_eq("1995-09-01").unwrap() & a.lt("1995-10-01").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN Node
    let hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("p_type").unwrap();
            let mask = a
                .utf8()
                .unwrap()
                .into_iter()
                .map(|x| x.unwrap().starts_with("PROMO") as i32)
                .collect_vec();
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let columns = vec![
                Series::new(
                    "denominator_promo_revenue",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64),
                ),
                Series::new(
                    "numerator_promo_revenue",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64)
                        * Series::new("promo", &mask),
                ),
            ];
            df.hstack(&columns).unwrap()
        })))
        .build();

    // AGGREGATE Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_aggregates(vec![
            ("numerator_promo_revenue".into(), vec!["sum".into()]),
            ("denominator_promo_revenue".into(), vec!["sum".into()]),
        ])
        .set_add_count_column(true);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();
    let scaler_node = AggregateScaler::new_growing()
        .remove_count_column()  // Remove added group count column
        .scale_sum("numerator_promo_revenue_sum".into())
        .scale_sum("denominator_promo_revenue_sum".into())
        .into_node();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let num = df.column("numerator_promo_revenue_sum").unwrap();
            let den = df.column("denominator_promo_revenue_sum").unwrap();
            let res = Series::new("promo_revenue", (num / den) * 100f64);
            DataFrame::new(vec![res]).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    hash_join_node.subscribe_to_node(&where_node, 0); // Left Node
    hash_join_node.subscribe_to_node(&part_csvreader_node, 1); // Right Node
    expression_node.subscribe_to_node(&hash_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    scaler_node.subscribe_to_node(&groupby_node, 0);
    select_node.subscribe_to_node(&scaler_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(where_node);
    service.add(part_csvreader_node);
    service.add(hash_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(scaler_node);
    service.add(select_node);
    service
}
