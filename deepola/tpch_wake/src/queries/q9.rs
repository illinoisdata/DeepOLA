use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::prelude::Utf8Methods;
use polars::series::IntoSeries;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query
// select
// 	nation,
// 	o_year,
// 	sum(amount) as sum_profit
// from
// 	(
// 		select
// 			n_name as nation,
// 			extract(year from o_orderdate) as o_year,
// 			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
// 		from
// 			part,
// 			supplier,
// 			lineitem,
// 			partsupp,
// 			orders,
// 			nation
// 		where
// 			s_suppkey = l_suppkey
// 			and ps_suppkey = l_suppkey
// 			and ps_partkey = l_partkey
// 			and p_partkey = l_partkey
// 			and o_orderkey = l_orderkey
// 			and s_nationkey = n_nationkey
// 			and p_name like '%green%'
// 	) as profit
// group by
// 	nation,
// 	o_year
// order by
// 	nation,
// 	o_year desc;

/// Refomrulated Query:
// select
//     n_name as nation,
//     extract(year from o_orderdate) as o_year,
//     sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
// from
//     part,
//     supplier,
//     lineitem,
//     partsupp,
//     orders,
//     nation
// where
//     s_suppkey = l_suppkey
//     and ps_suppkey = l_suppkey
//     and ps_partkey = l_partkey
//     and p_partkey = l_partkey
//     and o_orderkey = l_orderkey
//     and s_nationkey = n_nationkey
//     and p_name like '%green%'
// group by
// 	nation,
// 	o_year
// order by
// 	nation,
// 	o_year desc;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        ("nation".into(), vec!["n_nationkey", "n_name"]),
        ("part".into(), vec!["p_partkey", "p_name"]),
        (
            "partsupp".into(),
            vec!["ps_partkey", "ps_suppkey", "ps_supplycost"],
        ),
        ("supplier".into(), vec!["s_suppkey", "s_nationkey"]),
        ("orders".into(), vec!["o_orderkey", "o_orderdate"]),
        (
            "lineitem".into(),
            vec![
                "l_orderkey",
                "l_suppkey",
                "l_partkey",
                "l_extendedprice",
                "l_discount",
                "l_quantity",
            ],
        ),
    ]);

    // CSV Reader Nodes.
    let nation_csvreader_node = build_csv_reader_node("nation".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);
    let partsupp_csvreader_node =
        build_csv_reader_node("partsupp".into(), &tableinput, &table_columns);
    let supplier_csvreader_node =
        build_csv_reader_node("supplier".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let part_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_name = df.column("p_name").unwrap();
            let mask = p_name
                .utf8()
                .unwrap()
                .into_iter()
                .map(|opt_v| opt_v.map(|x| x.contains("green") as bool))
                .collect();
            df.filter(&mask).unwrap()
        })))
        .build();

    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    let ls_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    let lp_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    let lps_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into(), "l_suppkey".into()])
        .right_on(vec!["ps_partkey".into(), "ps_suppkey".into()])
        .build();

    let mut merger = SortedDfMerger::new();
    merger.set_left_on(vec!["l_orderkey".into()]);
    merger.set_right_on(vec!["o_orderkey".into()]);
    let lo_merge_join_node = MergerNode::<DataFrame, SortedDfMerger>::new()
        .merger(merger)
        .build();

    // Expression Node.
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let nation = Series::new("nation", df.column("n_name").unwrap());
            let o_orderdate = df.column("o_orderdate").unwrap();
            let o_year = Series::new(
                "o_year",
                o_orderdate
                    .utf8()
                    .unwrap()
                    .as_date(Some("%Y-%m-%d"))
                    .unwrap()
                    .strftime("%Y")
                    .into_series(),
            );

            let l_extendedprice = df.column("l_extendedprice").unwrap();
            let l_discount = df.column("l_discount").unwrap();
            let l_quantity = df.column("l_quantity").unwrap();
            let ps_supplycost = df.column("ps_supplycost").unwrap();
            let profit = Series::new(
                "profit",
                l_extendedprice
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (l_discount * -1f64 + 1f64)
                    - (ps_supplycost * l_quantity),
            );
            DataFrame::new(vec![nation, o_year, profit]).unwrap()
        })))
        .build();

    // GROUP BY Node.
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec!["nation".into(), "o_year".into()])
        .set_aggregates(vec![("profit".into(), vec!["sum".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // Select row and divide to get mkt_share
            df.sort(vec!["nation", "o_year"], vec![false, true])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    part_where_node.subscribe_to_node(&part_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&nation_csvreader_node, 1);
    ls_hash_join_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    ls_hash_join_node.subscribe_to_node(&sn_hash_join_node, 1);
    lp_hash_join_node.subscribe_to_node(&ls_hash_join_node, 0);
    lp_hash_join_node.subscribe_to_node(&part_where_node, 1);
    lps_hash_join_node.subscribe_to_node(&lp_hash_join_node, 0);
    lps_hash_join_node.subscribe_to_node(&partsupp_csvreader_node, 1);
    lo_merge_join_node.subscribe_to_node(&lps_hash_join_node, 0);
    lo_merge_join_node.subscribe_to_node(&orders_csvreader_node, 1);
    expression_node.subscribe_to_node(&lo_merge_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(nation_csvreader_node);
    service.add(lineitem_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(partsupp_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(part_csvreader_node);
    service.add(part_where_node);
    service.add(sn_hash_join_node);
    service.add(ls_hash_join_node);
    service.add(lp_hash_join_node);
    service.add(lps_hash_join_node);
    service.add(lo_merge_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
