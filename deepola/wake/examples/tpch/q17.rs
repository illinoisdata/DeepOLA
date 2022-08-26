use crate::utils::*;

extern crate runtime;
use runtime::graph::*;
use runtime::data::*;
use runtime::operations::*;

use std::collections::HashMap;

// select
// 	sum(l_extendedprice) / 7.0 as avg_yearly
// from
// 	lineitem,
// 	part
// where
// 	p_partkey = l_partkey
// 	and p_brand = 'Brand#23'
// 	and p_container = 'MED BOX'
// 	and l_quantity < (
// 		select
// 			0.2 * avg(l_quantity)
// 		from
// 			lineitem
// 		where
// 			l_partkey = p_partkey
// 	);

pub fn query(tableinput: HashMap<String, TableInput>, output_reader: &mut NodeReader<ArrayRow>) -> ExecutionService<ArrayRow> {
    // CSV Reader node
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);

    fn part_where_predicate(record: &ArrayRow) -> bool {
        (String::from(&record.values[3]) == "Brand#23".to_string()) &&
        (String::from(&record.values[6]) == "MED BOX".to_string())
    }
    let part_where_node = WhereNode::node(part_where_predicate);

    let hash_join_node = HashJoinNode::node(
        vec!["l_partkey".into()], // l_partkey on lineitem
        vec!["p_partkey".into()], // p_partkey on part
        JoinType::Inner
    );

    fn revenue_numerator(record: &ArrayRow) -> DataCell {
        if String::from(&record.values[19])[..5] == "PROMO".to_string() {
            DataCell::Float(f64::from(&record.values[5]) * (1.0 - f64::from(&record.values[6])))
        } else {
            DataCell::Float(0.0)
        }
    }
    fn revenue_denominator(record: &ArrayRow) -> DataCell {
        DataCell::Float(f64::from(&record.values[5]) * (1.0 - f64::from(&record.values[6])))
    }
    let expressions = vec![
        Expression {
            predicate: revenue_numerator,
            alias: "revenue_numerator".into(),
            dtype: DataType::Float
        },
        Expression {
            predicate: revenue_denominator,
            alias: "revenue_denominator".into(),
            dtype: DataType::Float
        }
    ];
    let expression_node = ExpressionNode::node(expressions);

    let aggregates = vec![
        Aggregate {
            column: "revenue_numerator".to_string(),
            operation: AggregationOperation::Sum,
            alias: Some("revenue_numerator".into()),
        },
        Aggregate {
            column: "revenue_denominator".to_string(),
            operation: AggregationOperation::Sum,
            alias: Some("revenue_denominator".into()),
        },
    ];
    let groupby_cols = vec![];
    let groupby_node = GroupByNode::node(groupby_cols, aggregates);

    fn promo_revenue(record: &ArrayRow) -> DataCell {
        DataCell::Float(100.0 * (f64::from(&record.values[0]) / f64::from(&record.values[1])))
    }
    let expressions_2 = vec![
        Expression {
            predicate: promo_revenue,
            alias: "promo_revenue".into(),
            dtype: DataType::Float
        }
    ];
    let expression_node_2 = ExpressionNode::node(expressions_2);

    let select_node_builder = SelectNodeBuilder::new(vec!["promo_revenue".to_string()]);
    let select_node = select_node_builder.build();

    // Connect nodes with subscription
    lineitem_where_node.subscribe_to_node(&lineitem_csvreader_node,0);
    hash_join_node.subscribe_to_node(&lineitem_where_node, 0);
    hash_join_node.subscribe_to_node(&part_csvreader_node, 1);
    expression_node.subscribe_to_node(&hash_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    expression_node_2.subscribe_to_node(&groupby_node, 0);
    select_node.subscribe_to_node(&expression_node_2, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<ArrayRow>::create();
    service.add(select_node);
    service.add(expression_node_2);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(hash_join_node);
    service.add(lineitem_where_node);
    service.add(lineitem_csvreader_node);
    service.add(part_csvreader_node);
    service
}