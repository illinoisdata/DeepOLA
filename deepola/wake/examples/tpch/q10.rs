use crate::utils::*;

extern crate runtime;
use runtime::data::*;
use runtime::graph::*;
use runtime::operations::*;

use std::cmp;
use std::collections::HashMap;

// select
// 	c_custkey,
// 	c_name,
// 	sum(l_extendedprice * (1 - l_discount)) as revenue,
// 	c_acctbal,
// 	n_name,
// 	c_address,
// 	c_phone,
// 	c_comment
// from
// 	customer,
// 	orders,
// 	lineitem,
// 	nation
// where
// 	c_custkey = o_custkey
// 	and l_orderkey = o_orderkey
// 	and o_orderdate >= date '1993-10-01'
// 	and o_orderdate < date '1993-10-01' + interval '3' month
// 	and l_returnflag = 'R'
// 	and c_nationkey = n_nationkey
// group by
// 	c_custkey,
// 	c_name,
// 	c_acctbal,
// 	c_phone,
// 	n_name,
// 	c_address,
// 	c_comment
// order by
// 	revenue desc;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<ArrayRow>,
) -> ExecutionService<ArrayRow> {
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec![
                "l_orderkey",
                "l_extendedprice",
                "l_discount",
                "l_returnflag",
            ],
        ),
        (
            "orders".into(),
            vec!["o_orderkey", "o_custkey", "o_orderdate"],
        ),
        (
            "customer".into(),
            vec![
                "c_custkey",
                "c_name",
                "c_address",
                "c_nationkey",
                "c_phone",
                "c_acctbal",
                "c_comment",
            ],
        ),
        ("nation".into(), vec!["n_nationkey", "n_name"]),
    ]);

    // CSV Reader node
    let lineitem_csvreader_node =
        build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node =
        build_csv_reader_node("customer".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_csv_reader_node("nation".into(), &tableinput, &table_columns);

    // WHERE node
    fn orders_predicate(record: &ArrayRow) -> bool {
        String::from(&record.values[2]) < "1994-01-01".to_string()
            && String::from(&record.values[2]) >= "1993-10-01".to_string()
    }
    let orders_where_node = WhereNode::node(orders_predicate);

    // Hash Join node
    let hash_join_node_customer_nation = HashJoinNode::node(
        vec!["c_nationkey".into()], //left is customer
        vec!["n_nationkey".into()], //right is nation
        JoinType::Inner,
    );
    let hash_join_node_order_customer = HashJoinNode::node(
        vec!["o_custkey".into()], //left is order
        vec!["c_custkey".into()], //right is joined_result
        JoinType::Inner,
    );

    // MERGE JOIN node
    let mut merge_join_node_builder = MergeJoinBuilder::new();
    merge_join_node_builder.left_on_index(vec![0]);
    merge_join_node_builder.right_on_index(vec![0]);
    let merge_join_node = merge_join_node_builder.build();

    // EXPRESSION node
    fn revenue_expression(record: &ArrayRow) -> DataCell {
        DataCell::Float(f64::from(&record.values[1]) * (1.0 - f64::from(&record.values[2])))
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
    let groupby_cols = vec![
        "o_custkey".into(),
        "c_name".into(),
        "c_acctbal".into(),
        "c_phone".into(),
        "n_name".into(),
        "c_address".into(),
        "c_comment".into(),
    ];
    let groupby_node = GroupByNode::node(groupby_cols, aggregates);

    // SELECT node
    let selected_cols = vec!["*".into()];
    let mut select_node_builder = SelectNodeBuilder::new(selected_cols);
    fn order_by_predicate(a: &ArrayRow, b: &ArrayRow) -> cmp::Ordering {
        if a.values[7] > b.values[7] {
            cmp::Ordering::Less
        } else if a.values[7] < b.values[7] {
            cmp::Ordering::Greater
        } else {
            cmp::Ordering::Equal
        }
    }
    select_node_builder.orderby(order_by_predicate);
    let select_node = select_node_builder.build();

    // Connect nodes with subscription
    orders_where_node.subscribe_to_node(&orders_csvreader_node, 0);
    hash_join_node_customer_nation.subscribe_to_node(&customer_csvreader_node, 0);
    hash_join_node_customer_nation.subscribe_to_node(&nation_csvreader_node, 1);
    hash_join_node_order_customer.subscribe_to_node(&orders_where_node, 0);
    hash_join_node_order_customer.subscribe_to_node(&hash_join_node_customer_nation, 1);
    merge_join_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    merge_join_node.subscribe_to_node(&hash_join_node_order_customer, 1);
    expression_node.subscribe_to_node(&merge_join_node, 0);
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
    service.add(merge_join_node);
    service.add(hash_join_node_order_customer);
    service.add(hash_join_node_customer_nation);
    service.add(orders_where_node);
    service.add(customer_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(lineitem_csvreader_node);
    service
}
