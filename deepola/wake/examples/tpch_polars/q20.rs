use crate::prelude::*;

/// This node implements the following SQL query
// select
// 	s_name,
// 	s_address
// from
// 	supplier,
// 	nation
// where
// 	s_suppkey in (
// 		select
// 			ps_suppkey
// 		from
// 			partsupp
// 		where
// 			ps_partkey in (
// 				select
// 					p_partkey
// 				from
// 					part
// 				where
// 					p_name like 'forest%'
// 			)
// 			and ps_availqty > (
// 				select
// 					0.5 * sum(l_quantity)
// 				from
// 					lineitem
// 				where
// 					l_partkey = ps_partkey
// 					and l_suppkey = ps_suppkey
// 					and l_shipdate >= date '1994-01-01'
// 					and l_shipdate < date '1994-01-01' + interval '1' year
// 			)
// 	)
// 	and s_nationkey = n_nationkey
// 	and n_name = 'CANADA'
// order by
// 	s_name;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_partkey", "l_suppkey", "l_shipdate", "l_quantity"],
        ),
        ("part".into(), vec!["p_partkey", "p_name"]),
        ("nation".into(), vec!["n_nationkey", "n_name"]),
        (
            "supplier".into(),
            vec!["s_nationkey", "s_suppkey", "s_name", "s_address"],
        ),
        (
            "partsupp".into(),
            vec!["ps_suppkey", "ps_partkey", "ps_availqty"],
        ),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_reader_node("part".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_reader_node("nation".into(), &tableinput, &table_columns);
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);
    let partsupp_csvreader_node = build_reader_node("partsupp".into(), &tableinput, &table_columns);

    let part_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_name = df.column("p_name").unwrap();
            let mask = p_name
                .utf8()
                .unwrap()
                .into_iter()
                .map(|opt_v| opt_v.map(|x| x.starts_with("forest") as bool))
                .collect();
            df.filter(&mask).unwrap()
        })))
        .build();

    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let var_date_1 = days_since_epoch(1994,1,1);
            let var_date_2 = days_since_epoch(1995,1,1);
            let l_shipdate = df.column("l_shipdate").unwrap();
            let mask =
                l_shipdate.gt_eq(var_date_1).unwrap() & l_shipdate.lt(var_date_2).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let nation_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let n_name = df.column("n_name").unwrap();
            let mask = n_name.equal("CANADA").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // GROUP BY Node
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["l_partkey".into(), "l_suppkey".into()])
        .set_aggregates(vec![("l_quantity".into(), vec!["sum".into()])]);
    let lineitem_aggregate = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // HASH JOIN Node
    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    let psp_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["ps_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    let pss_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["ps_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    let mut ps_accumulator = AggAccumulator::new();
    ps_accumulator
        .set_group_key(vec![
            "ps_partkey".into(),
            "ps_suppkey".into(),
            "ps_availqty".into(),
            "s_name".into(),
            "s_address".into(),
        ])
        .set_aggregates(vec![("s_nationkey".into(), vec!["count".into()])]);
    let ps_aggregate_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(ps_accumulator)
        .build();

    // The result is obtained when right goes EOF. Not when left does.
    let mut ps_availqty_merger = MapperDfMerger::new();
    ps_availqty_merger.set_mapper(Box::new(|left_df: &DataFrame, right_df: &DataFrame| {
        let joined_df = left_df
            .join(
                right_df,
                ["ps_partkey", "ps_suppkey"],
                ["l_partkey", "l_suppkey"],
                polars::prelude::JoinType::Inner,
                None,
            )
            .unwrap();
        let ps_availqty = joined_df.column("ps_availqty").unwrap();
        let ps_availqty_avg = joined_df.column("l_quantity_sum").unwrap() * 0.5f64;
        let mask = ps_availqty.gt(&ps_availqty_avg).unwrap();
        joined_df
            .filter(&mask)
            .unwrap()
            .select(["s_name", "s_address", "ps_suppkey"])
            .unwrap()
            .groupby(["s_name", "s_address"])
            .unwrap()
            .count()
            .unwrap()
    }));
    ps_availqty_merger.set_eof_behavior(MapperDfEOFBehavior::RightEOF);
    let ps_availqty_merger_node = MergerNode::<DataFrame, MapperDfMerger>::new()
        .merger(ps_availqty_merger)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let columns = vec!["s_name", "s_address"];
            df.select(columns)
                .unwrap()
                .sort(vec!["s_name"], vec![false])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    part_where_node.subscribe_to_node(&part_csvreader_node, 0);
    nation_where_node.subscribe_to_node(&nation_csvreader_node, 0);
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    lineitem_aggregate.subscribe_to_node(&lineitem_where_node, 0);

    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0); // Left Node
    sn_hash_join_node.subscribe_to_node(&nation_where_node, 1); // Right Node
    psp_hash_join_node.subscribe_to_node(&partsupp_csvreader_node, 0); // Left Node
    psp_hash_join_node.subscribe_to_node(&part_where_node, 1); // Right Node

    pss_hash_join_node.subscribe_to_node(&psp_hash_join_node, 0); // Left Node
    pss_hash_join_node.subscribe_to_node(&sn_hash_join_node, 1); // Right Node
    ps_aggregate_node.subscribe_to_node(&pss_hash_join_node, 0);

    ps_availqty_merger_node.subscribe_to_node(&ps_aggregate_node, 0);
    ps_availqty_merger_node.subscribe_to_node(&lineitem_aggregate, 1);
    select_node.subscribe_to_node(&ps_availqty_merger_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(part_csvreader_node);
    service.add(partsupp_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(part_where_node);
    service.add(nation_where_node);
    service.add(lineitem_where_node);
    service.add(lineitem_aggregate);
    service.add(sn_hash_join_node);
    service.add(psp_hash_join_node);
    service.add(pss_hash_join_node);
    service.add(ps_aggregate_node);
    service.add(ps_availqty_merger_node);
    service.add(select_node);
    service
}
