use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::series::ChunkCompare;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query
// select
// 	o_orderpriority,
// 	count(*) as order_count
// from
// 	orders
// where
// 	o_orderdate >= date '1993-07-01'
// 	and o_orderdate < date '1993-07-01' + interval '3' month
// 	and exists (
// 		select
// 			*
// 		from
// 			lineitem
// 		where
// 			l_orderkey = o_orderkey
// 			and l_commitdate < l_receiptdate
// 	)
// group by
// 	o_orderpriority
// order by
// 	o_orderpriority;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_orderkey", "l_commitdate", "l_receiptdate"],
        ),
        (
            "orders".into(),
            vec!["o_orderkey", "o_orderdate", "o_orderpriority"],
        ),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);

    // WHERE Node
    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let l_commitdate = df.column("l_commitdate").unwrap();
            let l_receiptdate = df.column("l_receiptdate").unwrap();
            let mask = l_commitdate.lt(l_receiptdate).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();
    let orders_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let o_orderdate = df.column("o_orderdate").unwrap();
            let mask =
                o_orderdate.gt_eq("1993-07-01").unwrap() & o_orderdate.lt("1993-10-01").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // Merge JOIN Node
    let mut merger = SortedDfMerger::new();
    merger.set_left_on(vec!["l_orderkey".into()]);
    merger.set_right_on(vec!["o_orderkey".into()]);
    let lo_merge_join_node = MergerNode::<DataFrame, SortedDfMerger>::new()
        .merger(merger)
        .build();

    // EXPRESSION Node (for distinct)
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.unique(
                Some(&["l_orderkey".into()]),
                polars::prelude::UniqueKeepStrategy::First,
            )
            .unwrap()
            .groupby(["o_orderpriority"])
            .unwrap()
            .agg(&[("l_orderkey", vec!["count"])])
            .unwrap()
        })))
        .build();

    // GROUP BY AGGREGATE Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec!["o_orderpriority".into()])
        .set_aggregates(vec![("l_orderkey_count".into(), vec!["sum".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.sort(&["o_orderpriority"], vec![false]).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    orders_where_node.subscribe_to_node(&orders_csvreader_node, 0);
    lo_merge_join_node.subscribe_to_node(&lineitem_where_node, 0); // Left Node
    lo_merge_join_node.subscribe_to_node(&orders_where_node, 1); // Right Node
    expression_node.subscribe_to_node(&lo_merge_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(lineitem_where_node);
    service.add(orders_where_node);
    service.add(lo_merge_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
