use crate::utils::*;

extern crate runtime;
use runtime::graph::*;
use runtime::data::*;
use runtime::operations::*;

use std::collections::HashMap;
use std::cmp;

// select
// 	l_orderkey,
// 	sum(l_extendedprice * (1 - l_discount)) as revenue,
// 	o_orderdate,
// 	o_shippriority
// from
// 	customer,
// 	orders,
// 	lineitem
// where
// 	c_mktsegment = 'BUILDING'
// 	and c_custkey = o_custkey
// 	and l_orderkey = o_orderkey
// 	and o_orderdate < date '1995-03-15'
// 	and l_shipdate > date '1995-03-15'
// group by
// 	l_orderkey,
// 	o_orderdate,
// 	o_shippriority
// order by
// 	revenue desc,
// 	o_orderdate;

pub fn query(tableinput: HashMap<String, TableInput>, output_reader: &mut NodeReader<ArrayRow>) -> ExecutionService<ArrayRow> {
    // CSV Reader node
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput);
    let customer_csvreader_node = build_csv_reader_node("customer".into(), &tableinput);

    // WHERE node
    fn lineitem_predicate(record: &ArrayRow) -> bool { String::from(&record.values[10]) > "1995-03-15".to_string() }
    fn orders_predicate(record: &ArrayRow) -> bool { String::from(&record.values[4]) < "1995-03-15".to_string() }
    fn customer_predicate(record: &ArrayRow) -> bool { String::from(&record.values[6]) == "BUILDING".to_string() }
    let lineitem_where_node = WhereNode::node(lineitem_predicate);
    let orders_where_node = WhereNode::node(orders_predicate);
    let customer_where_node = WhereNode::node(customer_predicate);

    // Merge Join node
    let mut merge_join_builder = MergeJoinBuilder::new();
    merge_join_builder.left_on_index(vec![0]);
    merge_join_builder.right_on_index(vec![0]);
    let merge_join_node = merge_join_builder.build();

    // Hash Join node
    let hash_join_node = HashJoinNode::node(
        vec![16], //left is joined result of lineitem and orders
        vec![0], //right is customer
        JoinType::Inner
    );

    // EXPRESSION node
    fn revenue_expression(record: &ArrayRow) -> DataCell {
        DataCell::Float(f64::from(&record.values[5]) * (1.0 - f64::from(&record.values[6])))
    }
    let expressions = vec![
        Expression {
            predicate: revenue_expression,
            alias: "revenue".into(),
            dtype: DataType::Float
        }
    ];
    let expression_node = ExpressionNode::node(expressions);

    // GROUP BY node
    let aggregates = vec![
        Aggregate {
            column: "revenue".to_string(),
            operation: AggregationOperation::Sum,
            alias: Some("revenue".into()),
        }
    ];
    let groupby_cols = vec!["l_orderkey".into(),"o_orderdate".into(),"o_shippriority".into()];
    let groupby_node = GroupByNode::node(groupby_cols, aggregates);

    // SELECT node
    let selected_cols = vec!["*".into()];
    let mut select_node_builder = SelectNodeBuilder::new(selected_cols);
    fn order_by_predicate(a: &ArrayRow, b: &ArrayRow) -> cmp::Ordering {
        if a.values[3] > b.values[3] {
            cmp::Ordering::Less
        } else if a.values[3] < b.values[3] {
            cmp::Ordering::Greater
        } else {
            if a.values[1] < b.values[1] {
                cmp::Ordering::Less
            } else if a.values[1] > b.values[1] {
                cmp::Ordering::Greater
            } else {
                cmp::Ordering::Equal
            }
        }
    }
    select_node_builder.orderby(order_by_predicate).limit(20);
    let select_node = select_node_builder.build();

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node,0);
    orders_where_node.subscribe_to_node(&orders_csvreader_node, 0);
    customer_where_node.subscribe_to_node(&customer_csvreader_node, 0);
    merge_join_node.subscribe_to_node(&lineitem_where_node, 0);
    merge_join_node.subscribe_to_node(&orders_where_node, 1);
    hash_join_node.subscribe_to_node(&merge_join_node, 0);
    hash_join_node.subscribe_to_node(&customer_where_node, 1);
    expression_node.subscribe_to_node(&hash_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node,0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<ArrayRow>::create();
    // Add all nodes
    service.add(select_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(hash_join_node);
    service.add(merge_join_node);
    service.add(customer_where_node);
    service.add(orders_where_node);
    service.add(lineitem_where_node);
    service.add(customer_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(lineitem_csvreader_node);
    service
}