use crate::prelude::*;

/// WanderJoin's modified Q7.
/// Refer: https://github.com/InitialDLab/XDB/blob/master/queries/query7_selection.sql
/// This node implements the following SQL query
// SELECT SUM(l_extendedprice * (1 - l_discount))
// FROM nation n1, supplier, lineitem, orders, customer, nation n2
// WHERE   s_suppkey = l_suppkey
//     AND o_orderkey = l_orderkey
//     AND c_custkey = o_custkey
//     AND s_nationkey = n1.n_nationkey
//     AND c_nationkey = n2.n_nationkey
// 	   AND n1.n_name = 'CHINA'
//     AND l_shipdate > date '1994-05-01';

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
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
    let nation_csvreader_node = build_reader_node("nation".into(), &tableinput, &table_columns);
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);
    let customer_csvreader_node = build_reader_node("customer".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let nation_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let n_name = df.column("n_name").unwrap();
            let mask = n_name.equal("CHINA").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // WHERE Node
    let lineitem_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let var_date_1 = days_since_epoch(1994,5,1);
            let l_shipdate = df.column("l_shipdate").unwrap();
            let mask = l_shipdate.gt(var_date_1).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN Node
    let cn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["c_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    let oc_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["o_custkey".into()])
        .right_on(vec!["c_custkey".into()])
        .build();

    let ls_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    // Merge JOIN Node
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
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator
        .set_aggregates(vec![("disc_price".into(), vec!["sum".into()])])
        .set_add_count_column(true)
        .set_scaler(AggregateScaler::new_growing()
            .remove_count_column()  // Remove added group count column
            .scale_sum("disc_price_sum".into())
            .into_rc()
        );

    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    cn_hash_join_node.subscribe_to_node(&customer_csvreader_node, 0);
    cn_hash_join_node.subscribe_to_node(&nation_csvreader_node, 1);
    nation_where_node.subscribe_to_node(&nation_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&nation_where_node, 1);
    ls_hash_join_node.subscribe_to_node(&lineitem_where_node, 0);
    ls_hash_join_node.subscribe_to_node(&sn_hash_join_node, 1);
    oc_hash_join_node.subscribe_to_node(&orders_csvreader_node, 0);
    oc_hash_join_node.subscribe_to_node(&cn_hash_join_node, 1);
    lo_merge_join_node.subscribe_to_node(&ls_hash_join_node, 0); // Left Node
    lo_merge_join_node.subscribe_to_node(&oc_hash_join_node, 1); // Right Node
    expression_node.subscribe_to_node(&lo_merge_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&groupby_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(nation_where_node);
    service.add(lineitem_where_node);
    service.add(cn_hash_join_node);
    service.add(sn_hash_join_node);
    service.add(oc_hash_join_node);
    service.add(ls_hash_join_node);
    service.add(lo_merge_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service
}
