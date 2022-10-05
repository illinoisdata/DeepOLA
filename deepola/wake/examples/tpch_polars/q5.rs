use crate::prelude::*;

/// This node implements the following SQL query
// select
// 	n_name,
// 	sum(l_extendedprice * (1 - l_discount)) as revenue
// from
// 	customer,
// 	orders,
// 	lineitem,
// 	supplier,
// 	nation,
// 	region
// where
// 	c_custkey = o_custkey
// 	and l_orderkey = o_orderkey
// 	and l_suppkey = s_suppkey
// 	and c_nationkey = s_nationkey
// 	and s_nationkey = n_nationkey
// 	and n_regionkey = r_regionkey
// 	and r_name = 'ASIA'
// 	and o_orderdate >= date '1994-01-01'
// 	and o_orderdate < date '1994-01-01' + interval '1' year
// group by
// 	n_name
// order by
// 	revenue desc;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        ("customer".into(), vec!["c_custkey", "c_nationkey"]),
        (
            "orders".into(),
            vec!["o_orderkey", "o_custkey", "o_orderdate"],
        ),
        (
            "lineitem".into(),
            vec!["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"],
        ),
        ("supplier".into(), vec!["s_suppkey", "s_nationkey"]),
        (
            "nation".into(),
            vec!["n_regionkey", "n_nationkey", "n_name"],
        ),
        ("region".into(), vec!["r_regionkey", "r_name"]),
    ]);

    // CSV Reader Nodes.
    let customer_csvreader_node = build_reader_node("customer".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node = build_reader_node("region".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let orders_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let var_date_1 = days_since_epoch(1994,1,1);
            let var_date_2 = days_since_epoch(1995,1,1);
            let o_orderdate = df.column("o_orderdate").unwrap();
            let mask =
                o_orderdate.gt_eq(var_date_1).unwrap() & o_orderdate.lt(var_date_2).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let region_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let r_name = df.column("r_name").unwrap();
            let mask = r_name.equal("ASIA").unwrap();
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

    let ls_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    let oc_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["o_custkey".into(), "s_nationkey".into()])
        .right_on(vec!["c_custkey".into(), "c_nationkey".into()])
        .build();

    let mut merger = SortedDfMerger::new();
    merger.set_left_on(vec!["l_orderkey".into()]);
    merger.set_right_on(vec!["o_orderkey".into()]);
    let lo_merge_join_node = MergerNode::<DataFrame, SortedDfMerger>::new()
        .merger(merger)
        .build();

    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let disc_price = Series::new(
                "disc_price",
                extended_price
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (discount * -1f64 + 1f64),
            );
            df.hstack(&[disc_price]).unwrap()
        })))
        .build();

    // GROUP BY AGGREGATE Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec!["n_name".into()])
        .set_aggregates(vec![("disc_price".into(), vec!["sum".into()])])
        .set_add_count_column(true)
        .set_scaler(AggregateScaler::new_growing()
            .remove_count_column()  // Remove added group count column
            .scale_sum("disc_price_sum".into())
            .into_rc()
        );
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.sort(vec!["disc_price_sum"], vec![true]).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    region_where_node.subscribe_to_node(&region_csvreader_node, 0);
    orders_where_node.subscribe_to_node(&orders_csvreader_node, 0);
    nr_hash_join_node.subscribe_to_node(&nation_csvreader_node, 0);
    nr_hash_join_node.subscribe_to_node(&region_where_node, 1);
    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&nr_hash_join_node, 1);
    ls_hash_join_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    ls_hash_join_node.subscribe_to_node(&sn_hash_join_node, 1);
    lo_merge_join_node.subscribe_to_node(&ls_hash_join_node, 0);
    lo_merge_join_node.subscribe_to_node(&orders_where_node, 1);
    oc_hash_join_node.subscribe_to_node(&lo_merge_join_node, 0);
    oc_hash_join_node.subscribe_to_node(&customer_csvreader_node, 1);
    expression_node.subscribe_to_node(&oc_hash_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(orders_csvreader_node);
    service.add(region_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(lineitem_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(region_where_node);
    service.add(orders_where_node);
    service.add(nr_hash_join_node);
    service.add(sn_hash_join_node);
    service.add(ls_hash_join_node);
    service.add(oc_hash_join_node);
    service.add(lo_merge_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}
