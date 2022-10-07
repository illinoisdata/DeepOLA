use crate::prelude::*;

/// This node implements the following SQL query
// select
// 	ps_partkey,
// 	sum(ps_supplycost * ps_availqty) as value
// from
// 	partsupp,
// 	supplier,
// 	nation
// where
// 	ps_suppkey = s_suppkey
// 	and s_nationkey = n_nationkey
// 	and n_name = 'GERMANY'
// group by
// 	ps_partkey having
// 		value > (
// 			select
// 				sum(ps_supplycost * ps_availqty) * 0.0001000000
// 			from
// 				partsupp,
// 				supplier,
// 				nation
// 			where
// 				ps_suppkey = s_suppkey
// 				and s_nationkey = n_nationkey
// 				and n_name = 'GERMANY'
// 		)
// order by
// 	value desc;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "partsupp".into(),
            vec!["ps_partkey", "ps_suppkey", "ps_supplycost", "ps_availqty"],
        ),
        ("supplier".into(), vec!["s_suppkey", "s_nationkey"]),
        ("nation".into(), vec!["n_nationkey", "n_name"]),
    ]);

    // CSVReaderNode would be created for this table.
    let partsupp_csvreader_node = build_reader_node("partsupp".into(), &tableinput, &table_columns);
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_reader_node("nation".into(), &tableinput, &table_columns);

    // WHERE Node
    let nation_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let n_name = df.column("n_name").unwrap();
            let mask = n_name.equal("GERMANY").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN Node
    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    let pss_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["ps_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // ps_supplycost * ps_availqty
            let ps_supplycost = df.column("ps_supplycost").unwrap();
            let ps_availqty = df.column("ps_availqty").unwrap();
            let value = Series::new(
                "value",
                ps_availqty
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * ps_supplycost
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap(),
            );
            let fraction_value = Series::new("value_percent", value.clone() * 0.0001f64);
            df.hstack(&[value, fraction_value]).unwrap()
        })))
        .build();

    // GROUP BY AGGREGATE Node
    let mut having_accumulator = AggAccumulator::new();
    having_accumulator.set_aggregates(vec![("value_percent".into(), vec!["sum".into()])]);
    let having_groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(having_accumulator)
        .build();

    // GROUP BY AGGREGATE Node
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["ps_partkey".into()])
        .set_aggregates(vec![("value".into(), vec!["sum".into()])]);
    let sum_groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // MapperDfMerger - Merges result of MAIN QUERY AND HAVING SUBQUERY
    let mut having_merger = MapperDfMerger::new();
    having_merger.set_mapper(Box::new(|left_df: &DataFrame, right_df: &DataFrame| {
        let value_sum = left_df.column("value_sum").unwrap();
        let percent_value = right_df.column("value_percent_sum").unwrap();
        let mask = (value_sum.gt(percent_value)).unwrap();
        left_df.filter(&mask).unwrap()
    }));
    let having_node = MergerNode::<DataFrame, MapperDfMerger>::new()
        .merger(having_merger)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.sort(&["value_sum"], vec![true]).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    nation_where_node.subscribe_to_node(&nation_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&nation_where_node, 1);
    pss_hash_join_node.subscribe_to_node(&partsupp_csvreader_node, 0);
    pss_hash_join_node.subscribe_to_node(&sn_hash_join_node, 1);
    expression_node.subscribe_to_node(&pss_hash_join_node, 0);
    sum_groupby_node.subscribe_to_node(&expression_node, 0);
    having_groupby_node.subscribe_to_node(&expression_node, 0);
    having_node.subscribe_to_node(&sum_groupby_node, 0); // Left DF
    having_node.subscribe_to_node(&having_groupby_node, 1); // Right DF
    select_node.subscribe_to_node(&having_node, 0); // Right DF

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(partsupp_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(nation_where_node);
    service.add(sn_hash_join_node);
    service.add(pss_hash_join_node);
    service.add(expression_node);
    service.add(sum_groupby_node);
    service.add(having_groupby_node);
    service.add(having_node);
    service.add(select_node);
    service
}
//
