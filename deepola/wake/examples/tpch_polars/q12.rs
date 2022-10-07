use crate::prelude::*;

/// This node implements the following SQL query
// select
// 	l_shipmode,
// 	sum(case
// 		when o_orderpriority = '1-URGENT'
// 			or o_orderpriority = '2-HIGH'
// 			then 1
// 		else 0
// 	end) as high_line_count,
// 	sum(case
// 		when o_orderpriority <> '1-URGENT'
// 			and o_orderpriority <> '2-HIGH'
// 			then 1
// 		else 0
// 	end) as low_line_count
// from
// 	orders,
// 	lineitem
// where
// 	o_orderkey = l_orderkey
// 	and l_shipmode in ('MAIL', 'SHIP')
// 	and l_commitdate < l_receiptdate
// 	and l_shipdate < l_commitdate
// 	and l_receiptdate >= date '1994-01-01'
// 	and l_receiptdate < date '1994-01-01' + interval '1' year
// group by
// 	l_shipmode
// order by
// 	l_shipmode;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        ("orders".into(), vec!["o_orderkey", "o_orderpriority"]),
        (
            "lineitem".into(),
            vec![
                "l_orderkey",
                "l_shipmode",
                "l_commitdate",
                "l_shipdate",
                "l_receiptdate",
            ],
        ),
    ]);

    // CSV Reader Nodes.
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let l_shipmode = df.column("l_shipmode").unwrap();
            let l_commitdate = df.column("l_commitdate").unwrap();
            let l_receiptdate = df.column("l_receiptdate").unwrap();
            let l_shipdate = df.column("l_shipdate").unwrap();
            let var_date_1 = days_since_epoch(1994,1,1);
            let var_date_2 = days_since_epoch(1995,1,1);
            let mask = (l_shipmode.equal("MAIL").unwrap() | l_shipmode.equal("SHIP").unwrap())
                & l_commitdate.lt(l_receiptdate).unwrap()
                & l_shipdate.lt(l_commitdate).unwrap()
                & l_receiptdate.gt_eq(var_date_1).unwrap()
                & l_receiptdate.lt(var_date_2).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let mut merger = SortedDfMerger::new();
    merger.set_left_on(vec!["l_orderkey".into()]);
    merger.set_right_on(vec!["o_orderkey".into()]);
    let lo_merge_join_node = MergerNode::<DataFrame, SortedDfMerger>::new()
        .merger(merger)
        .build();

    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let o_orderpriority = df.column("o_orderpriority").unwrap();
            let high_line_count = Series::new(
                "high_line",
                o_orderpriority
                    .utf8()
                    .unwrap()
                    .into_iter()
                    .map(|opt_v| opt_v.map(|x| (x == "1-URGENT" || x == "2-HIGH") as bool))
                    .collect_vec(),
            );
            let low_line_count = Series::new(
                "low_line",
                o_orderpriority
                    .utf8()
                    .unwrap()
                    .into_iter()
                    .map(|opt_v| opt_v.map(|x| (x != "1-URGENT" && x != "2-HIGH") as bool))
                    .collect_vec(),
            );
            df.hstack(&[high_line_count, low_line_count]).unwrap()
        })))
        .build();

    // GROUP BY AGGREGATE Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec!["l_shipmode".into()])
        .set_aggregates(vec![
            ("high_line".into(), vec!["sum".into()]),
            ("low_line".into(), vec!["sum".into()]),
        ])
        .set_add_count_column(true)
        .set_scaler(AggregateScaler::new_growing()
            .remove_count_column()  // Remove added group count column
            .scale_sum("high_line_sum".into())
            .scale_sum("low_line_sum".into())
            .into_rc()
        );
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.sort(vec!["l_shipmode"], vec![false]).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    lo_merge_join_node.subscribe_to_node(&lineitem_where_node, 0);
    lo_merge_join_node.subscribe_to_node(&orders_csvreader_node, 1);
    expression_node.subscribe_to_node(&lo_merge_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(orders_csvreader_node);
    service.add(lineitem_csvreader_node);
    service.add(lineitem_where_node);
    service.add(lo_merge_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
