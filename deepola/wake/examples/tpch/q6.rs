use crate::utils::*;

extern crate runtime;
use runtime::graph::*;
use runtime::data::*;
use runtime::operations::*;

use std::collections::HashMap;

// select
// 	sum(l_extendedprice * l_discount) as revenue
// from
// 	lineitem
// where
// 	l_shipdate >= date '1994-01-01'
// 	and l_shipdate < date '1994-01-01' + interval '1' year
// 	and l_discount between .06 - 0.01 and .06 + 0.01
// 	and l_quantity < 24;

pub fn query(tableinput: HashMap<String, TableInput>, output_reader: &mut NodeReader<ArrayRow>) -> ExecutionService<ArrayRow> {
    let table_columns = HashMap::from([
        ("lineitem".into(), vec!["l_quantity","l_extendedprice","l_discount","l_shipdate"]),
    ]);

    // CSV Reader node
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);

    fn where_predicate(record: &ArrayRow) -> bool {
        (String::from(&record.values[3]) >= "1994-01-01".to_string()) &&
        (String::from(&record.values[3]) < "1995-01-01".to_string()) &&
        (i32::from(&record.values[0]) < 24) &&
        (f64::from(&record.values[2]) < 0.06 + 0.01) &&
        (f64::from(&record.values[2]) > 0.06 - 0.01)
    }
    let lineitem_where_node = WhereNode::node(where_predicate);

    fn revenue_expression(record: &ArrayRow) -> DataCell {
        DataCell::Float(f64::from(&record.values[1]) * f64::from(&record.values[2]))
    }
    let expressions = vec![
        Expression {
            predicate: revenue_expression,
            alias: "revenue".into(),
            dtype: DataType::Float
        }
    ];
    let expression_node = ExpressionNode::node(expressions);

    let aggregates = vec![
        Aggregate {
            column: "revenue".to_string(),
            operation: AggregationOperation::Sum,
            alias: Some("revenue".into()),
        }
    ];
    let groupby_cols = vec![];
    let groupby_node = GroupByNode::node(groupby_cols, aggregates);

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node,0);
    expression_node.subscribe_to_node(&lineitem_where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&groupby_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<ArrayRow>::create();
    // Add all nodes
    service.add(groupby_node);
    service.add(expression_node);
    service.add(lineitem_where_node);
    service.add(lineitem_csvreader_node);
    service
}