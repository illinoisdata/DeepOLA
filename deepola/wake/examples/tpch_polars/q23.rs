use crate::prelude::*;

/// WanderJoin's modified Q3 by dropping select and groupby.
/// This node implements the following SQL query
// select
//  sum(l_extendedprice * (1 - l_discount)) as revenue,
//  COUNT(*)
// from
//  customer,
//  orders,
//  lineitem
// where
//  c_mktsegment = 'BUILDING'
//  and c_custkey = o_custkey
//  and l_orderkey = o_orderkey

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
        ),
        (
            "orders".into(),
            vec!["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"],
        ),
        ("customer".into(), vec!["c_custkey", "c_mktsegment"]),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node = build_reader_node("customer".into(), &tableinput, &table_columns);

    // WHERE Node
    let customer_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let a = df.column("c_mktsegment").unwrap();
            let mask = a.equal("BUILDING").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN Node
    let oc_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["o_custkey".into()])
        .right_on(vec!["c_custkey".into()])
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
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_aggregates(vec![
            ("disc_price".into(), vec!["sum".into()]),
            ("l_orderkey".into(), vec!["count".into()]),
        ])
        .set_scaler(AggregateScaler::new_growing()
            .count_column("l_orderkey_count".into())
            .scale_count("l_orderkey_count".into())
            .scale_sum("disc_price_sum".into())
            .into_rc()
        );
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();

    // Connect nodes with subscription
    customer_where_node.subscribe_to_node(&customer_csvreader_node, 0);
    oc_hash_join_node.subscribe_to_node(&orders_csvreader_node, 0); // Left Node
    oc_hash_join_node.subscribe_to_node(&customer_where_node, 1); // Right Node
    lo_merge_join_node.subscribe_to_node(&lineitem_csvreader_node, 0); // Left Node
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
    service.add(customer_where_node);
    service.add(oc_hash_join_node);
    service.add(lo_merge_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service
}
