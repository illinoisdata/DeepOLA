use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::JoinType;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

/// This node implements the following SQL query
// select
// 	cntrycode,
// 	count(*) as numcust,
// 	sum(c_acctbal) as totacctbal
// from
// 	(
// 		select
// 			substring(c_phone from 1 for 2) as cntrycode,
// 			c_acctbal
// 		from
// 			customer
// 		where
// 			substring(c_phone from 1 for 2) in
// 				('13', '31', '23', '29', '30', '18', '17')
// 			and c_acctbal > (
// 				select
// 					avg(c_acctbal)
// 				from
// 					customer
// 				where
// 					c_acctbal > 0.00
// 					and substring(c_phone from 1 for 2) in
// 						('13', '31', '23', '29', '30', '18', '17')
// 			)
// 			and not exists (
// 				select
// 					*
// 				from
// 					orders
// 				where
// 					o_custkey = c_custkey
// 			)
// 	) as custsale
// group by
// 	cntrycode
// order by
// 	cntrycode;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        ("orders".into(), vec!["o_custkey", "o_orderkey"]),
        ("customer".into(), vec!["c_custkey", "c_phone", "c_acctbal"]),
    ]);

    // CSVReaderNode would be created for this table.
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node = build_reader_node("customer".into(), &tableinput, &table_columns);

    // WHERE Node
    let customer_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let c_acctbal = df.column("c_acctbal").unwrap();
            let c_phone = df.column("c_phone").unwrap();
            let cntrycode = Series::new(
                "cntrycode",
                c_phone
                    .utf8()
                    .unwrap()
                    .into_iter()
                    .map(|opt_v| match opt_v {
                        Some(a) => &a[..2],
                        None => "",
                    })
                    .collect::<Vec<&str>>(),
            );
            let mask = c_acctbal.gt(0i32).unwrap()
                & cntrycode
                    .utf8()
                    .unwrap()
                    .into_iter()
                    .map(|opt_v| {
                        opt_v.map(|x| {
                            vec!["13", "31", "23", "29", "30", "18", "17"].contains(&x) as bool
                        })
                    })
                    .collect();
            let modified_df = df
                .select(vec!["c_custkey", "c_acctbal"])
                .unwrap()
                .hstack(&[cntrycode])
                .unwrap();
            modified_df.filter(&mask).unwrap()
        })))
        .build();

    // Combine all customer CSV files into one.
    let mut merge_accumulator = MergeAccumulator::new();
    merge_accumulator.set_merge_strategy(MergeAccumulatorStrategy::VStack);
    let customer_combine_node = AccumulatorNode::<DataFrame, MergeAccumulator>::new()
        .accumulator(merge_accumulator)
        .build();

    // GROUP BY Node
    let mut avg_accumulator = AggAccumulator::new();
    avg_accumulator.set_aggregates(vec![(
        "c_acctbal".into(),
        vec!["sum".into(), "count".into()],
    )]);
    let avg_groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(avg_accumulator)
        .build();

    // Filter by ACCTBAL
    let mut customer_acctbal_merger = MapperDfMerger::new();
    customer_acctbal_merger.set_mapper(Box::new(|left_df: &DataFrame, right_df: &DataFrame| {
        // left_df is combined customer table.
        // right_df is computed sum and count.
        let avg_acctbal =
            right_df.column("c_acctbal_sum").unwrap() / right_df.column("c_acctbal_count").unwrap();
        let mask = left_df
            .column("c_acctbal")
            .unwrap()
            .gt(&avg_acctbal)
            .unwrap();
        left_df.filter(&mask).unwrap()
    }));
    let customer_acctbal_merger_node = MergerNode::<DataFrame, MapperDfMerger>::new()
        .merger(customer_acctbal_merger)
        .build();

    // GROUP BY Node
    let mut o_custkey_accumulator = AggAccumulator::new();
    o_custkey_accumulator
        .set_group_key(vec!["o_custkey".into()])
        .set_aggregates(vec![("o_orderkey".into(), vec!["count".into()])]);
    let o_custkey_groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(o_custkey_accumulator)
        .build();

    // Remove c_custkey from the current order partition.
    let mut orders_customer_merger = MapperDfMerger::new();
    orders_customer_merger.set_mode(MapperDfMergerMode::LeftOnline);
    orders_customer_merger.set_mapper(Box::new(|left_df: &DataFrame, right_df: &DataFrame| {
        let joined_df = right_df
            .join(
                left_df,
                vec!["c_custkey"],
                vec!["o_custkey"],
                JoinType::Left,
                None,
            )
            .unwrap();
        let mask = joined_df.column("o_orderkey_count").unwrap().is_null();
        joined_df
            .select(vec!["c_custkey", "c_acctbal", "cntrycode"])
            .unwrap()
            .filter(&mask)
            .unwrap()
    }));
    let orders_customer_merger_node = MergerNode::<DataFrame, MapperDfMerger>::new()
        .merger(orders_customer_merger)
        .build();

    // Perform final groupby
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            df.groupby(vec!["cntrycode"])
                .unwrap()
                .agg(&[("c_custkey", &["count"]), ("c_acctbal", &["sum"])])
                .unwrap()
                .sort(vec!["cntrycode"], vec![false])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    customer_where_node.subscribe_to_node(&customer_csvreader_node, 0); // Left Node
    customer_combine_node.subscribe_to_node(&customer_where_node, 0); // Right Node
    avg_groupby_node.subscribe_to_node(&customer_combine_node, 0);
    customer_acctbal_merger_node.subscribe_to_node(&customer_combine_node, 0);
    customer_acctbal_merger_node.subscribe_to_node(&avg_groupby_node, 1);
    o_custkey_groupby_node.subscribe_to_node(&orders_csvreader_node, 0);
    orders_customer_merger_node.subscribe_to_node(&o_custkey_groupby_node, 0);
    orders_customer_merger_node.subscribe_to_node(&customer_acctbal_merger_node, 1);
    select_node.subscribe_to_node(&orders_customer_merger_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(select_node);
    service.add(orders_customer_merger_node);
    service.add(customer_acctbal_merger_node);
    service.add(avg_groupby_node);
    service.add(o_custkey_groupby_node);
    service.add(customer_combine_node);
    service.add(customer_where_node);
    service.add(orders_csvreader_node);
    service.add(customer_csvreader_node);
    service
}
