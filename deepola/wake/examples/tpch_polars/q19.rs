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
// 	sum(l_extendedprice* (1 - l_discount)) as revenue
// from
// 	lineitem,
// 	part
// where
// 	(
// 		p_partkey = l_partkey
// 		and p_brand = 'Brand#12'
// 		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
// 		and l_quantity >= 1 and l_quantity <= 1 + 10
// 		and p_size between 1 and 5
// 		and l_shipmode in ('AIR', 'AIR REG')
// 		and l_shipinstruct = 'DELIVER IN PERSON'
// 	)
// 	or
// 	(
// 		p_partkey = l_partkey
// 		and p_brand = 'Brand#23'
// 		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
// 		and l_quantity >= 10 and l_quantity <= 10 + 10
// 		and p_size between 1 and 10
// 		and l_shipmode in ('AIR', 'AIR REG')
// 		and l_shipinstruct = 'DELIVER IN PERSON'
// 	)
// 	or
// 	(
// 		p_partkey = l_partkey
// 		and p_brand = 'Brand#34'
// 		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
// 		and l_quantity >= 20 and l_quantity <= 20 + 10
// 		and p_size between 1 and 15
// 		and l_shipmode in ('AIR', 'AIR REG')
// 		and l_shipinstruct = 'DELIVER IN PERSON'
// 	);

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec![
                "l_partkey",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_shipinstruct",
                "l_shipmode",
            ],
        ),
        (
            "part".into(),
            vec!["p_partkey", "p_brand", "p_size", "p_container"],
        ),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_reader_node("part".into(), &tableinput, &table_columns);

    // HASH JOIN Node
    let hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_brand = df.column("p_brand").unwrap();
            let p_container = df.column("p_container").unwrap();
            let p_size = df.column("p_size").unwrap();
            let l_quantity = df.column("l_quantity").unwrap();
            let l_shipmode = df.column("l_shipmode").unwrap();
            let l_shipinstruct = df.column("l_shipinstruct").unwrap();

            let common_mask = p_size.gt_eq(1i32).unwrap()
                & l_shipinstruct.equal("DELIVER IN PERSON").unwrap()
                & l_shipmode
                    .utf8()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|v| vec!["AIR", "AIR REG"].contains(&v) as bool)
                    .collect();

            let mask1 = p_brand.equal("Brand#12").unwrap()
                & p_size.lt_eq(5i32).unwrap()
                & l_quantity.gt_eq(1i32).unwrap()
                & l_quantity.lt_eq(11i32).unwrap()
                & p_container
                    .utf8()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|v| vec!["SM CASE", "SM BOX", "SM PACK", "SM PKG"].contains(&v) as bool)
                    .collect();

            let mask2 = p_brand.equal("Brand#23").unwrap()
                & p_size.lt_eq(10i32).unwrap()
                & l_quantity.gt_eq(10i32).unwrap()
                & l_quantity.lt_eq(20i32).unwrap()
                & p_container
                    .utf8()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|v| vec!["MED BAG", "MED BOX", "MED PKG", "MED PACK"].contains(&v) as bool)
                    .collect();

            let mask3 = p_brand.equal("Brand#34").unwrap()
                & p_size.lt_eq(15i32).unwrap()
                & l_quantity.gt_eq(20i32).unwrap()
                & l_quantity.lt_eq(30i32).unwrap()
                & p_container
                    .utf8()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|v| vec!["LG CASE", "LG BOX", "LG PACK", "LG PKG"].contains(&v) as bool)
                    .collect();

            let mask = common_mask & (mask1 | mask2 | mask3);
            df.filter(&mask).unwrap()
        })))
        .build();

    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let columns = vec![Series::new(
                "disc_price",
                extended_price
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (discount * -1f64 + 1f64),
            )];
            DataFrame::new(columns).unwrap()
        })))
        .build();

    // GROUP BY Node
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator
        .set_aggregates(vec![("disc_price".into(), vec!["sum".into()])])
        .set_add_count_column(true);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();
    let scaler_node = AggregateScaler::new_growing()
        .remove_count_column()  // Remove added group count column
        .scale_sum("disc_price_sum".into())
        .into_node();

    // Connect nodes with subscription
    hash_join_node.subscribe_to_node(&lineitem_csvreader_node, 0); // Left Node
    hash_join_node.subscribe_to_node(&part_csvreader_node, 1); // Right Node
    where_node.subscribe_to_node(&hash_join_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    scaler_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&scaler_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(scaler_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(hash_join_node);
    service.add(part_csvreader_node);
    service.add(lineitem_csvreader_node);
    service
}
