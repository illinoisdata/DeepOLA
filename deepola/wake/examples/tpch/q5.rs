use crate::utils::*;

extern crate runtime;
use runtime::data::*;
use runtime::graph::*;
use runtime::operations::*;

use std::cmp;
use std::collections::HashMap;

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
    output_reader: &mut NodeReader<ArrayRow>,
) -> ExecutionService<ArrayRow> {
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"],
        ),
        (
            "orders".into(),
            vec!["o_orderkey", "o_custkey", "o_orderdate"],
        ),
        ("customer".into(), vec!["c_custkey", "c_nationkey"]),
        ("supplier".into(), vec!["s_suppkey", "s_nationkey"]),
        (
            "nation".into(),
            vec!["n_nationkey", "n_name", "n_regionkey"],
        ),
        ("region".into(), vec!["r_regionkey", "r_name"]),
    ]);

    // CSV Reader node
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node =
        build_csv_reader_node("customer".into(), &tableinput, &table_columns);
    let supplier_csvreader_node =
        build_csv_reader_node("supplier".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_csv_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node = build_csv_reader_node("region".into(), &tableinput, &table_columns);

    fn region_predicate(record: &ArrayRow) -> bool {
        // r_name == "ASIA"
        String::from(&record.values[1]) == "ASIA"
    }
    let region_predicate_node = WhereNode::node(region_predicate);
    fn order_predicate(record: &ArrayRow) -> bool {
        // 	and o_orderdate >= date '1994-01-01'
        // 	and o_orderdate < date '1994-01-01' + interval '1' year
        String::from(&record.values[2]) >= "1994-01-01".to_string()
            && String::from(&record.values[2]) < "1995-01-01".to_string()
    }
    let order_predicate_node = WhereNode::node(order_predicate);

    // JOIN nodes
    let nation_region_join_node = HashJoinNode::node(
        vec!["r_regionkey".into()],
        vec!["n_regionkey".into()],
        JoinType::Inner,
    );
    let supplier_nation_join_node = HashJoinNode::node(
        vec!["n_nationkey".into()],
        vec!["s_nationkey".into()],
        JoinType::Inner,
    );
    let lineitem_supplier_join_node = HashJoinNode::node(
        vec!["l_suppkey".into()],
        vec!["s_suppkey".into()],
        JoinType::Inner,
    );
    let order_customer_join_node = HashJoinNode::node(
        vec!["o_custkey".into(), "n_nationkey".into()],
        vec!["c_custkey".into(), "c_nationkey".into()],
        JoinType::Inner,
    );
    let mut merge_join_node_builder = MergeJoinBuilder::new();
    merge_join_node_builder.left_on_index(vec![0]);
    merge_join_node_builder.right_on_index(vec![0]);
    let lineitem_order_join_node = merge_join_node_builder.build();

    // Connect nodes with subscription
    order_predicate_node.subscribe_to_node(&orders_csvreader_node, 0);
    region_predicate_node.subscribe_to_node(&region_csvreader_node, 0);

    nation_region_join_node.subscribe_to_node(&region_predicate_node, 0);
    nation_region_join_node.subscribe_to_node(&nation_csvreader_node, 1);

    supplier_nation_join_node.subscribe_to_node(&nation_region_join_node, 0);
    supplier_nation_join_node.subscribe_to_node(&supplier_csvreader_node, 1);

    lineitem_supplier_join_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    lineitem_supplier_join_node.subscribe_to_node(&supplier_nation_join_node, 1);

    // This can run in parallel
    lineitem_order_join_node.subscribe_to_node(&lineitem_supplier_join_node, 0);
    lineitem_order_join_node.subscribe_to_node(&order_predicate_node, 1);

    // Now join Order with Customer
    order_customer_join_node.subscribe_to_node(&lineitem_order_join_node, 0);
    order_customer_join_node.subscribe_to_node(&customer_csvreader_node, 1);

    // EXPRESSION node
    fn revenue_expression(record: &ArrayRow) -> DataCell {
        DataCell::Float(f64::from(&record.values[2]) * (1.0 - f64::from(&record.values[3])))
    }
    let expressions = vec![Expression {
        predicate: revenue_expression,
        alias: "revenue".into(),
        dtype: DataType::Float,
    }];
    let expression_node = ExpressionNode::node(expressions);

    // GROUP BY node
    let aggregates = vec![Aggregate {
        column: "revenue".to_string(),
        operation: AggregationOperation::Sum,
        alias: Some("revenue".into()),
    }];
    let groupby_cols = vec!["n_name".into()];
    let groupby_node = GroupByNode::node(groupby_cols, aggregates);

    // ORDER BY node
    let selected_cols = vec!["*".into()];
    let mut select_node_builder = SelectNodeBuilder::new(selected_cols);
    fn order_by_predicate(a: &ArrayRow, b: &ArrayRow) -> cmp::Ordering {
        if a.values[1] > b.values[1] {
            cmp::Ordering::Less
        } else if a.values[1] < b.values[1] {
            cmp::Ordering::Greater
        } else {
            cmp::Ordering::Equal
        }
    }
    select_node_builder.orderby(order_by_predicate);
    let select_node = select_node_builder.build();

    expression_node.subscribe_to_node(&order_customer_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<ArrayRow>::create();
    // Add all nodes
    service.add(select_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(lineitem_order_join_node);
    service.add(order_customer_join_node);
    service.add(lineitem_supplier_join_node);
    service.add(order_predicate_node);
    service.add(supplier_nation_join_node);
    service.add(nation_region_join_node);
    service.add(region_predicate_node);
    service.add(lineitem_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(region_csvreader_node);
    service
}
