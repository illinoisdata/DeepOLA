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
// 	sum(l_extendedprice) / 7.0 as avg_yearly
// from
// 	lineitem,
// 	part
// where
// 	p_partkey = l_partkey
// 	and p_brand = 'Brand#23'
// 	and p_container = 'MED BOX'
// 	and l_quantity < (
// 		select
// 			0.2 * avg(l_quantity)
// 		from
// 			lineitem
// 		where
// 			l_partkey = p_partkey
// 	);

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_partkey", "l_quantity", "l_extendedprice"],
        ),
        ("part".into(), vec!["p_partkey", "p_brand", "p_container"]),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);

    // FIRST GROUP BY AGGREGATE NODE.
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["l_partkey".to_string()])
        .set_aggregates(vec![(
            "l_quantity".into(),
            vec!["sum".into(), "count".into()],
        )]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let cols = vec![
                Series::new("l_partkey", df.column("l_partkey").unwrap()),
                Series::new(
                    "l_quantity_avg",
                    (df.column("l_quantity_sum")
                        .unwrap()
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap())
                        * 0.2f64
                        / (df
                            .column("l_quantity_count")
                            .unwrap()
                            .cast(&polars::datatypes::DataType::Float64)
                            .unwrap()),
                ),
            ];
            DataFrame::new(cols).unwrap()
        })))
        .build();

    let part_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_brand = df.column("p_brand").unwrap();
            let p_container = df.column("p_container").unwrap();
            let mask = p_brand.equal("Brand#23").unwrap() & p_container.equal("MED BOX").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN Node
    let lp_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    let ll_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["l_partkey".into()])
        .build();

    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let l_quantity = df.column("l_quantity").unwrap();
            let l_quantity_avg = df.column("l_quantity_avg").unwrap();
            let mask = l_quantity.lt(l_quantity_avg).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // AGGREGATE Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_aggregates(vec![("l_extendedprice".into(), vec!["sum".into()])])
        .set_add_count_column(true);
    let final_groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();
    let scaler_node = AggregateScaler::new_growing()
        .remove_count_column()  // Remove added group count column
        .scale_sum("l_extendedprice_sum".into())
        .into_node();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            DataFrame::new(vec![Series::new(
                "avg_yearly",
                df.column("l_extendedprice_sum").unwrap() / 7.0f64,
            )])
            .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    groupby_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    expression_node.subscribe_to_node(&groupby_node, 0);
    part_where_node.subscribe_to_node(&part_csvreader_node, 0);
    lp_hash_join_node.subscribe_to_node(&expression_node, 0);
    lp_hash_join_node.subscribe_to_node(&part_where_node, 1);
    ll_hash_join_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    ll_hash_join_node.subscribe_to_node(&lp_hash_join_node, 1);
    lineitem_where_node.subscribe_to_node(&ll_hash_join_node, 0);
    final_groupby_node.subscribe_to_node(&lineitem_where_node, 0);
    scaler_node.subscribe_to_node(&final_groupby_node, 0);
    select_node.subscribe_to_node(&scaler_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(part_csvreader_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(part_where_node);
    service.add(lp_hash_join_node);
    service.add(ll_hash_join_node);
    service.add(lineitem_where_node);
    service.add(final_groupby_node);
    service.add(scaler_node);
    service.add(select_node);
    service
}
