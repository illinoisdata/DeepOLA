use crate::utils::*;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
extern crate wake;
use polars::prelude::DataFrame;
use polars::series::Series;
use std::collections::HashMap;
use wake::graph::*;
use wake::polars_operations::*;

/// This node implements the following SQL query
// create view revenue0 (supplier_no, total_revenue) as
// 	select
// 		l_suppkey,
// 		sum(l_extendedprice * (1 - l_discount))
// 	from
// 		lineitem
// 	where
// 		l_shipdate >= date '1996-01-01'
// 		and l_shipdate < date '1996-01-01' + interval '3' month
// 	group by
// 		l_suppkey;
// select
// 	s_suppkey,
// 	s_name,
// 	s_address,
// 	s_phone,
// 	total_revenue
// from
// 	supplier,
// 	revenue0
// where
// 	s_suppkey = supplier_no
// 	and total_revenue = (
// 		select
// 			max(total_revenue)
// 		from
// 			revenue0
// 	)
// order by
// 	s_suppkey;
// drop view revenue0;

// Modified Query
// select
// 	s_suppkey,
// 	s_name,
// 	s_address,
// 	s_phone,
//  sum(l_extendedprice * (1 - l_discount)) as total_revenue
// from
//     lineitem,
// 	   supplier
// where
//     s_suppkey = l_suppkey
//     and l_shipdate >= date '1996-01-01'
//     and l_shipdate < date '1996-01-01' + interval '3' month
// group by
//     l_suppkey

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_suppkey", "l_extendedprice", "l_discount", "l_shipdate"],
        ),
        (
            "supplier".into(),
            vec!["s_suppkey", "s_name", "s_address", "s_phone"],
        ),
    ]);

    // CSV Reader Nodes.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let l_shipdate = df.column("l_shipdate").unwrap();
            let mask =
                l_shipdate.gt_eq("1996-01-01").unwrap() & l_shipdate.lt("1996-04-01").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // EXPRESSION Nodes
    let lineitem_expr_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let total_revenue = Series::new(
                "total_revenue",
                extended_price
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (discount * -1f64 + 1f64),
            );
            df.hstack(&[total_revenue]).unwrap()
        })))
        .build();

    // HASH JOIN Node
    let ls_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    // GROUP BY AGGREGATE Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec![
            "l_suppkey".into(),
            "s_name".into(),
            "s_address".into(),
            "s_phone".into(),
        ])
        .set_aggregates(vec![("total_revenue".into(), vec!["sum".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let total_revenue_sum = df.column("total_revenue_sum").unwrap();
            let max_total_revenue: f64 = total_revenue_sum.max().unwrap();
            let mask = total_revenue_sum.equal(max_total_revenue).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    lineitem_expr_node.subscribe_to_node(&lineitem_where_node, 0);
    ls_hash_join_node.subscribe_to_node(&lineitem_expr_node, 0);
    ls_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 1);
    groupby_node.subscribe_to_node(&ls_hash_join_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(lineitem_where_node);
    service.add(lineitem_expr_node);
    service.add(ls_hash_join_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
