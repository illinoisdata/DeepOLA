use std::rc::Rc;

use crate::prelude::*;
use wake::processor::MessageFractionProcessor;


/// This node implements the following types of SQL query
// SELECT sum(t.x) as x
// FROM (
//   SELECT max(t.x) as x
//   FROM (
//     SELECT sum(t.x) as x
//     FROM (
//       SELECT max(t.x) as x
//       FROM   numbertable as t
//       GROUP BY c, ci, cii
//     ) AS t
//     GROUP BY c, ci
//   ) AS t
//   GROUP BY c
// ) AS t

fn make_scaler(aggregate_column: &str, aggregate_op: &str) -> Rc<dyn MessageFractionProcessor<DataFrame>> {
    match aggregate_op {
        "sum" => {
            AggregateScaler::new_growing()
                .remove_count_column()  // Remove added group count column
                .scale_sum(aggregate_column.into())
                .into_rc()
        },
        "max" => {
            AggregateScaler::new_growing()
                .remove_count_column()  // Remove added group count column
                .into_rc()
        },
        _ => panic!("Unsupported aggregate operator"),
    }
}

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
    query_depth: usize
) -> ExecutionService<polars::prelude::DataFrame> {
    let agg_cycle = vec!["sum", "max"];  // configurable?

    // Table Name => Columns to Read.
    let gb_columns: Vec<String> = (1 .. query_depth + 1)
        .map(|idx| "c".to_string() + &"i".repeat(idx))
        .collect();
    let gb_columns_borrow: Vec<&str> = gb_columns.iter().map(|s| &s[..]).collect();
    let numbertable_columns = HashMap::from([
        ("numbertable".into(), [gb_columns_borrow, vec!["x"]].concat()),
    ]);

    // CSV Reader Nodes.
    let numbertable_csvreader_node = build_reader_node("numbertable".into(), &tableinput, &numbertable_columns);

    // GROUP BY AGGREGATE Nodes
    let mut groupby_nodes = Vec::new();
    let mut value_column = "x".to_string();
    for depth in (0 .. query_depth + 1).rev() {
        let mut agg_accumulator = AggAccumulator::new();
        let aggregate_op = agg_cycle[depth % agg_cycle.len()];
        let aggregate_column = format!("{}_{}", value_column, aggregate_op);
        agg_accumulator
            .set_group_key(gb_columns[..depth].to_vec())
            .set_aggregates(vec![(value_column.clone(), vec![aggregate_op.to_string()])])
            .set_add_count_column(true)
            .set_scaler(make_scaler(&aggregate_column, &aggregate_op));
        let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
            .accumulator(agg_accumulator)
            .build();
        groupby_nodes.push(groupby_node);
        value_column = aggregate_column;
    }

    // Connect nodes with subscription
    groupby_nodes.first().unwrap().subscribe_to_node(&numbertable_csvreader_node, 0);
    for groupby_node_pair in groupby_nodes.windows(2) {
        assert_eq!(groupby_node_pair.len(), 2);
        let groupby_node_1 = &groupby_node_pair[0];
        let groupby_node_2 = &groupby_node_pair[1];
        groupby_node_2.subscribe_to_node(&groupby_node_1, 0);
    }

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&groupby_nodes.last().unwrap(), 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(numbertable_csvreader_node);
    for groupby_node in groupby_nodes {
        service.add(groupby_node);
    }
    service
}
