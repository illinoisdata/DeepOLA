use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::prelude::Utf8Methods;
use polars::series::ChunkCompare;
use polars::series::IntoSeries;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query
// select
// 	supp_nation,
// 	cust_nation,
// 	l_year,
// 	sum(volume) as revenue
// from
// 	(
// 		select
// 			n1.n_name as supp_nation,
// 			n2.n_name as cust_nation,
// 			extract(year from l_shipdate) as l_year,
// 			l_extendedprice * (1 - l_discount) as volume
// 		from
// 			supplier,
// 			lineitem,
// 			orders,
// 			customer,
// 			nation n1,
// 			nation n2
// 		where
// 			s_suppkey = l_suppkey
// 			and o_orderkey = l_orderkey
// 			and c_custkey = o_custkey
// 			and s_nationkey = n1.n_nationkey
// 			and c_nationkey = n2.n_nationkey
// 			and (
// 				(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
// 				or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
// 			)
// 			and l_shipdate between date '1995-01-01' and date '1996-12-31'
// 	) as shipping
// group by
// 	supp_nation,
// 	cust_nation,
// 	l_year
// order by
// 	supp_nation,
// 	cust_nation,
// 	l_year;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        ("nation".into(), vec!["n_nationkey", "n_name"]),
        ("customer".into(), vec!["c_custkey", "c_nationkey"]),
        ("supplier".into(), vec!["s_suppkey", "s_nationkey"]),
        ("orders".into(), vec!["o_orderkey", "o_custkey"]),
        (
            "lineitem".into(),
            vec![
                "l_orderkey",
                "l_suppkey",
                "l_shipdate",
                "l_extendedprice",
                "l_discount",
            ],
        ),
    ]);

    // CSV Reader Nodes.
    let nation_csvreader_node = build_csv_reader_node("nation".into(), &tableinput, &table_columns);
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let supplier_csvreader_node =
        build_csv_reader_node("supplier".into(), &tableinput, &table_columns);
    let customer_csvreader_node =
        build_csv_reader_node("customer".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let nation_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let n_name = df.column("n_name").unwrap();
            let mask = n_name.equal("FRANCE").unwrap() | n_name.equal("GERMANY").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let l_shipdate = df.column("l_shipdate").unwrap();
            let mask =
                l_shipdate.gt_eq("1995-01-01").unwrap() & l_shipdate.lt_eq("1996-12-31").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // After this rename, n_name to supp_nation
    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();
    let sn_expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let cols = vec![
                Series::new("s_suppkey", df.column("s_suppkey").unwrap()),
                Series::new("supp_nation", df.column("n_name").unwrap()),
            ];
            DataFrame::new(cols).unwrap()
        })))
        .build();

    // After this rename, c_name to cust_nation
    let cn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["c_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();
    let cn_expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let cols = vec![
                Series::new("c_custkey", df.column("c_custkey").unwrap()),
                Series::new("cust_nation", df.column("n_name").unwrap()),
            ];
            DataFrame::new(cols).unwrap()
        })))
        .build();

    let oc_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["o_custkey".into()])
        .right_on(vec!["c_custkey".into()])
        .build();

    let ls_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    let mut merger = SortedDfMerger::new();
    merger.set_left_on(vec!["l_orderkey".into()]);
    merger.set_right_on(vec!["o_orderkey".into()]);
    let lo_merge_join_node = MergerNode::<DataFrame, SortedDfMerger>::new()
        .merger(merger)
        .build();

    // Expression Node.
    let lineitem_expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let supp_nation = df.column("supp_nation").unwrap();
            let cust_nation = df.column("cust_nation").unwrap();
            let mask = (supp_nation.equal("FRANCE").unwrap()
                & cust_nation.equal("GERMANY").unwrap())
                | (supp_nation.equal("GERMANY").unwrap() & cust_nation.equal("FRANCE").unwrap());
            let df = df.filter(&mask).unwrap();

            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let disc_price = Series::new(
                "disc_price",
                extended_price
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (discount * -1f64 + 1f64),
            );
            let l_shipdate = df.column("l_shipdate").unwrap();
            let l_year = Series::new(
                "l_year",
                l_shipdate
                    .utf8()
                    .unwrap()
                    .as_date(Some("%Y-%m-%d"))
                    .unwrap()
                    .strftime("%Y")
                    .into_series(),
            );
            df.hstack(&[disc_price, l_year]).unwrap()
        })))
        .build();

    // GROUP BY Node.
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec![
            "supp_nation".into(),
            "cust_nation".into(),
            "l_year".into(),
        ])
        .set_aggregates(vec![("disc_price".into(), vec!["sum".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.sort(
                vec!["supp_nation", "cust_nation", "l_year"],
                vec![false, false, false],
            )
            .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    nation_where_node.subscribe_to_node(&nation_csvreader_node, 0);
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node, 0);

    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&nation_where_node, 1);
    sn_expression_node.subscribe_to_node(&sn_hash_join_node, 0);
    ls_hash_join_node.subscribe_to_node(&lineitem_where_node, 0);
    ls_hash_join_node.subscribe_to_node(&sn_expression_node, 1);

    cn_hash_join_node.subscribe_to_node(&customer_csvreader_node, 0);
    cn_hash_join_node.subscribe_to_node(&nation_where_node, 1);
    cn_expression_node.subscribe_to_node(&cn_hash_join_node, 0);
    oc_hash_join_node.subscribe_to_node(&orders_csvreader_node, 0);
    oc_hash_join_node.subscribe_to_node(&cn_expression_node, 1);

    lo_merge_join_node.subscribe_to_node(&ls_hash_join_node, 0);
    lo_merge_join_node.subscribe_to_node(&oc_hash_join_node, 1);
    lineitem_expression_node.subscribe_to_node(&lo_merge_join_node, 0);

    groupby_node.subscribe_to_node(&lineitem_expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(nation_csvreader_node);
    service.add(lineitem_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(nation_where_node);
    service.add(lineitem_where_node);
    service.add(sn_hash_join_node);
    service.add(sn_expression_node);
    service.add(cn_hash_join_node);
    service.add(cn_expression_node);
    service.add(ls_hash_join_node);
    service.add(oc_hash_join_node);
    service.add(lo_merge_join_node);
    service.add(lineitem_expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
