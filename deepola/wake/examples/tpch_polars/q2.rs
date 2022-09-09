use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::series::ChunkCompare;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query
// select
// 	s_acctbal,
// 	s_name,
// 	n_name,
// 	p_partkey,
// 	p_mfgr,
// 	s_address,
// 	s_phone,
// 	s_comment
// from
// 	part,
// 	supplier,
// 	partsupp,
// 	nation,
// 	region
// where
// 	p_partkey = ps_partkey
// 	and s_suppkey = ps_suppkey
// 	and p_size = 15
// 	and p_type like '%BRASS'
// 	and s_nationkey = n_nationkey
// 	and n_regionkey = r_regionkey
// 	and r_name = 'EUROPE'
// 	and ps_supplycost = (
// 		select
// 			min(ps_supplycost)
// 		from
// 			partsupp,
// 			supplier,
// 			nation,
// 			region
// 		where
// 			p_partkey = ps_partkey
// 			and s_suppkey = ps_suppkey
// 			and s_nationkey = n_nationkey
// 			and n_regionkey = r_regionkey
// 			and r_name = 'EUROPE'
// 	)
// order by
// 	s_acctbal desc,
// 	n_name,
// 	s_name,
// 	p_partkey
// limit 100;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        (
            "part".into(),
            vec!["p_partkey", "p_size", "p_type", "p_mfgr"],
        ),
        (
            "supplier".into(),
            vec![
                "s_suppkey",
                "s_name",
                "s_address",
                "s_nationkey",
                "s_phone",
                "s_acctbal",
                "s_comment",
            ],
        ),
        (
            "partsupp".into(),
            vec!["ps_partkey", "ps_suppkey", "ps_supplycost"],
        ),
        (
            "nation".into(),
            vec!["n_regionkey", "n_nationkey", "n_name"],
        ),
        ("region".into(), vec!["r_regionkey", "r_name"]),
    ]);

    // CSV Reader Nodes.
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);
    let supplier_csvreader_node =
        build_csv_reader_node("supplier".into(), &tableinput, &table_columns);
    let partsupp_csvreader_node =
        build_csv_reader_node("partsupp".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_csv_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node = build_csv_reader_node("region".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let region_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let r_name = df.column("r_name").unwrap();
            let mask = r_name.equal("EUROPE").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let nr_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["n_regionkey".into()])
        .right_on(vec!["r_regionkey".into()])
        .build();

    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    let pss_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["ps_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    // GROUP BY Aggregate Node
    let mut min_accumulator = AggAccumulator::new();
    min_accumulator
        .set_group_key(vec!["ps_partkey".into()])
        .set_aggregates(vec![("ps_supplycost".into(), vec!["min".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(min_accumulator)
        .build();

    let part_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_size = df.column("p_size").unwrap();
            let p_type = df.column("p_type").unwrap();
            let mask = p_size.equal(15i32).unwrap()
                & p_type
                    .utf8()
                    .unwrap()
                    .into_iter()
                    .map(|opt_v| opt_v.map(|x| x.ends_with("BRASS") as bool))
                    .collect();
            df.filter(&mask).unwrap()
        })))
        .build();

    let psp_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["ps_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    let psps_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["ps_partkey".into(), "ps_supplycost".into()])
        .right_on(vec!["ps_partkey".into(), "ps_supplycost_min".into()])
        .build();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let cols = vec![
                "s_acctbal",
                "s_name",
                "n_name",
                "ps_partkey",
                "p_mfgr",
                "s_address",
                "s_phone",
                "s_comment",
            ];
            df.select(&cols)
                .unwrap()
                .sort(
                    vec!["s_acctbal", "n_name", "s_name", "ps_partkey"],
                    vec![true, false, false, false],
                )
                .unwrap()
                .slice(0, 100)
        })))
        .build();

    // Connect nodes with subscription
    part_where_node.subscribe_to_node(&part_csvreader_node, 0);
    region_where_node.subscribe_to_node(&region_csvreader_node, 0);
    nr_hash_join_node.subscribe_to_node(&nation_csvreader_node, 0);
    nr_hash_join_node.subscribe_to_node(&region_where_node, 1);
    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&nr_hash_join_node, 1);
    pss_hash_join_node.subscribe_to_node(&partsupp_csvreader_node, 0);
    pss_hash_join_node.subscribe_to_node(&sn_hash_join_node, 1);
    groupby_node.subscribe_to_node(&pss_hash_join_node, 0);
    psp_hash_join_node.subscribe_to_node(&pss_hash_join_node, 0);
    psp_hash_join_node.subscribe_to_node(&part_where_node, 1);
    psps_hash_join_node.subscribe_to_node(&psp_hash_join_node, 0);
    psps_hash_join_node.subscribe_to_node(&groupby_node, 1);

    select_node.subscribe_to_node(&psps_hash_join_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(part_csvreader_node);
    service.add(region_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(partsupp_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(region_where_node);
    service.add(nr_hash_join_node);
    service.add(sn_hash_join_node);
    service.add(pss_hash_join_node);
    service.add(groupby_node);
    service.add(psps_hash_join_node);
    service.add(part_where_node);
    service.add(psp_hash_join_node);
    service.add(select_node);
    service
}
