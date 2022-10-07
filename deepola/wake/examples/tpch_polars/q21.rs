use crate::prelude::*;

/// This node implements the following SQL query
// select
// 	s_name,
// 	count(*) as numwait
// from
// 	supplier,
// 	lineitem l1,
// 	orders,
// 	nation
// where
// 	s_suppkey = l1.l_suppkey
// 	and o_orderkey = l1.l_orderkey
// 	and o_orderstatus = 'F'
// 	and l1.l_receiptdate > l1.l_commitdate
// 	and exists (
// 		select
// 			*
// 		from
// 			lineitem l2
// 		where
// 			l2.l_orderkey = l1.l_orderkey
// 			and l2.l_suppkey <> l1.l_suppkey
// 	)
// 	and not exists (
// 		select
// 			*
// 		from
// 			lineitem l3
// 		where
// 			l3.l_orderkey = l1.l_orderkey
// 			and l3.l_suppkey <> l1.l_suppkey
// 			and l3.l_receiptdate > l3.l_commitdate
// 	)
// 	and s_nationkey = n_nationkey
// 	and n_name = 'SAUDI ARABIA'
// group by
// 	s_name
// order by
// 	numwait desc,
// 	s_name;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"],
        ),
        ("orders".into(), vec!["o_orderkey", "o_orderstatus"]),
        (
            "supplier".into(),
            vec!["s_suppkey", "s_nationkey", "s_name"],
        ),
        ("nation".into(), vec!["n_nationkey", "n_name"]),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_reader_node("nation".into(), &tableinput, &table_columns);

    // WHERE Node
    // o_orderstatus = 'F'
    let orders_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let o_orderstatus = df.column("o_orderstatus").unwrap();
            let mask = o_orderstatus.equal("F").unwrap();
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
    // let lo_merge_join_node = HashJoinBuilder::new()
    //     .left_on(vec!["l_orderkey".into()])
    //     .right_on(vec!["o_orderkey".into()])
    //     .build();

    // WHERE Node
    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let l_receiptdate = df.column("l_receiptdate").unwrap();
            let l_commitdate = df.column("l_commitdate").unwrap();
            let mask = l_receiptdate.gt(l_commitdate).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // orderkey WHERE node
    let mut orderkey_merger = MapperDfMerger::new();
    orderkey_merger.set_mapper(Box::new(|left_df: &DataFrame, right_df: &DataFrame| {
        // left_df is the lineitem node
        // right_df is the lineitem where node.
        let grouped_df1 = left_df
            .select(["l_orderkey", "l_suppkey"])
            .unwrap()
            .groupby(["l_orderkey"])
            .unwrap()
            .n_unique()
            .unwrap();
        let grouped_df2 = right_df
            .select(["l_orderkey", "l_suppkey"])
            .unwrap()
            .groupby(["l_orderkey"])
            .unwrap()
            .n_unique()
            .unwrap();
        let joined_df = grouped_df1
            .join(
                &grouped_df2,
                ["l_orderkey"],
                ["l_orderkey"],
                polars::prelude::JoinType::Inner,
                None,
            )
            .unwrap();
        //.sort(vec!["l_orderkey"], vec![false]).unwrap();
        let mask = joined_df
            .column("l_suppkey_n_unique")
            .unwrap()
            .gt(1u32)
            .unwrap()
            & joined_df
                .column("l_suppkey_n_unique_right")
                .unwrap()
                .equal(1u32)
                .unwrap();
        joined_df.filter(&mask).unwrap()
    }));
    let orderkey_merger_node = MergerNode::<DataFrame, MapperDfMerger>::new()
        .merger(orderkey_merger)
        .build();

    // WHERE NODE.
    let nation_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let n_name = df.column("n_name").unwrap();
            let mask = n_name.equal("SAUDI ARABIA").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN Node
    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    // HASH JOIN Node
    let ls_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    // MapperDfMerger
    let mut supplier_merger = MapperDfMerger::new();
    supplier_merger.set_mapper(Box::new(|left_df: &DataFrame, right_df: &DataFrame| {
        // left_df is filtered df.
        // right_df is orderkey df.
        // This JOIN is different from hash_join and merge_join
        // Since, hash_join requires to wait on right channel till EOF.
        // merge_join requires right_channel and left_channel to both be sorted.
        // In this case it can work, if we sort in the previous node. But we actually don't need a sort.
        let joined_df = left_df
            .join(
                right_df,
                ["l_orderkey"],
                ["l_orderkey"],
                JoinType::Inner,
                None,
            )
            .unwrap();
        // This is result on this partition.
        joined_df
            .groupby(["s_name", "l_orderkey"])
            .unwrap()
            .count()
            .unwrap()
    }));
    let supplier_merger_node = MergerNode::<DataFrame, MapperDfMerger>::new()
        .merger(supplier_merger)
        .build();

    // GROUP BY Accumulator Node
    let mut accumulator1 = AggAccumulator::new();
    accumulator1
        .set_group_key(vec!["s_name".into()])
        .set_aggregates(vec![("l_orderkey".into(), vec!["count".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(accumulator1)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.sort(&["l_orderkey_count", "s_name"], vec![true, false])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    orders_where_node.subscribe_to_node(&orders_csvreader_node, 0);
    lo_merge_join_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    lo_merge_join_node.subscribe_to_node(&orders_where_node, 1);
    lineitem_where_node.subscribe_to_node(&lo_merge_join_node, 0);
    orderkey_merger_node.subscribe_to_node(&lo_merge_join_node, 0);
    orderkey_merger_node.subscribe_to_node(&lineitem_where_node, 1);
    nation_where_node.subscribe_to_node(&nation_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&nation_where_node, 1);
    ls_hash_join_node.subscribe_to_node(&lineitem_where_node, 0);
    ls_hash_join_node.subscribe_to_node(&sn_hash_join_node, 1);
    supplier_merger_node.subscribe_to_node(&ls_hash_join_node, 0);
    supplier_merger_node.subscribe_to_node(&orderkey_merger_node, 1);
    groupby_node.subscribe_to_node(&supplier_merger_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(nation_where_node);
    service.add(sn_hash_join_node);
    service.add(orders_where_node);
    service.add(ls_hash_join_node);
    service.add(lo_merge_join_node);
    service.add(lineitem_where_node);
    service.add(orderkey_merger_node);
    service.add(supplier_merger_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
