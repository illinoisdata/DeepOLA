use crate::utils::*;
extern crate wake;
use polars::prelude::DataFrame;
use polars::series::ChunkCompare;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query
// select
// 	c_name,
// 	c_custkey,
// 	o_orderkey,
// 	o_orderdate,
// 	o_totalprice,
// 	sum(l_quantity)
// from
// 	customer,
// 	orders,
// 	lineitem
// where
// 	c_custkey = o_custkey
// 	and o_orderkey = l_orderkey
// group by
// 	c_name,
// 	c_custkey,
// 	o_orderkey,
// 	o_orderdate,
// 	o_totalprice
// having
//     sum(l_quantity) > 300
// order by
// 	o_totalprice desc,
// 	o_orderdate;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_orderkey", "l_quantity"],
        ),
        ("orders".into(), vec!["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"]),
        ("customer".into(), vec!["c_custkey", "c_name"]),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node =
        build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node =
        build_csv_reader_node("customer".into(), &tableinput, &table_columns);

    let oc_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["o_custkey".into()])
        .right_on(vec!["c_custkey".into()])
        .build();

    // Merge JOIN Node
    let mut merger = SortedDfMerger::new();
    merger.set_left_on(vec!["l_orderkey".into()]);
    merger.set_right_on(vec!["o_orderkey".into()]);
    let lo_merge_join_node = MergerNode::<DataFrame, SortedDfMerger>::new().merger(merger).build();

    // FIRST GROUP BY AGGREGATE NODE.
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator.set_group_key(vec!["c_name".into(),"o_custkey".into(),"l_orderkey".into(),"o_orderdate".into(),"o_totalprice".into()]).set_aggregates(vec![
        ("l_quantity".into(), vec!["sum".into()])
    ]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let mask = df.column("l_quantity_sum").unwrap().gt(300).unwrap();
            df.filter(&mask).unwrap().sort(vec!["o_totalprice", "o_orderdate"], vec![true, false]).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    oc_hash_join_node.subscribe_to_node(&orders_csvreader_node, 0);
    oc_hash_join_node.subscribe_to_node(&customer_csvreader_node, 1);
    lo_merge_join_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    lo_merge_join_node.subscribe_to_node(&oc_hash_join_node, 1);
    groupby_node.subscribe_to_node(&lo_merge_join_node, 0);
    where_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&where_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(lo_merge_join_node);
    service.add(oc_hash_join_node);
    service.add(groupby_node);
    service.add(where_node);
    service
}
