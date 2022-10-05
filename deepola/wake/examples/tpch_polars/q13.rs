use crate::prelude::*;

/// This node implements the following SQL query
// select
// 	c_count,
// 	count(*) as custdist
// from
// 	(
// 		select
// 			c_custkey,
// 			count(o_orderkey)
// 		from
// 			customer left outer join orders on
// 				c_custkey = o_custkey
// 				and o_comment not like '%special%requests%'
// 		group by
// 			c_custkey
// 	) as c_orders (c_custkey, c_count)
// group by
// 	c_count
// order by
// 	custdist desc,
// 	c_count desc;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        (
            "orders".into(),
            vec!["o_orderkey", "o_custkey", "o_comment"],
        ),
        ("customer".into(), vec!["c_custkey"]),
    ]);

    // CSV Reader Nodes.
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node = build_reader_node("customer".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let orders_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let o_comment = df.column("o_comment").unwrap();
            lazy_static! {
                static ref MY_REGEX: Regex = Regex::new(r".*special.*requests.*").unwrap();
            }
            let mask = o_comment
                .utf8()
                .unwrap()
                .into_iter()
                .map(|opt_v| opt_v.map(|x| !MY_REGEX.is_match(x)))
                .collect();
            df.filter(&mask).unwrap()
        })))
        .build();

    let oc_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["o_custkey".into()])
        .right_on(vec!["c_custkey".into()])
        .join_type(JoinType::Left)
        .swap(true)
        .build();

    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let o_orderkey = df.column("o_orderkey").unwrap();
            let o_orderkey_unit = Series::new("o_orderkey_unit", o_orderkey.is_not_null());
            df.hstack(&[o_orderkey_unit]).unwrap()
        })))
        .build();

    // GROUP BY AGGREGATE Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec!["c_custkey".into()])
        .set_aggregates(vec![("o_orderkey_unit".into(), vec!["sum".into()])])
        .set_add_count_column(true)
        .set_scaler(AggregateScaler::new_growing()
            .remove_count_column()  // Remove added group count column
            .scale_sum("o_orderkey_unit_sum".into())
            .into_rc()
        );
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();


    // Perform Repeated GROUP BY Again
    let final_groupby_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df| {
            let mut df = df.clone();
            df.apply("o_orderkey_unit_sum", |column| {
                column.cast(&polars::datatypes::DataType::UInt32).unwrap()
            }).expect("Failed to cast to integer");
            df.groupby(vec!["o_orderkey_unit_sum"])
                .unwrap()
                .agg(&[("c_custkey", &vec!["count"])])
                .unwrap()
        })))
        .build();
    // let final_scaler_node = AggregateScaler::new_complete()
    //     .count_column("c_custkey_count".into())
    //     .scale_count("c_custkey_count".into())
    //     .into_node();  // no-op

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.sort(
                vec!["c_custkey_count", "o_orderkey_unit_sum"],
                vec![true, true],
            )
            .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    orders_where_node.subscribe_to_node(&orders_csvreader_node, 0);
    oc_hash_join_node.subscribe_to_node(&orders_where_node, 0);
    oc_hash_join_node.subscribe_to_node(&customer_csvreader_node, 1);
    expression_node.subscribe_to_node(&oc_hash_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    final_groupby_node.subscribe_to_node(&groupby_node, 0);
    select_node.subscribe_to_node(&final_groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(orders_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(orders_where_node);
    service.add(oc_hash_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(final_groupby_node);
    service.add(select_node);
    service
}
