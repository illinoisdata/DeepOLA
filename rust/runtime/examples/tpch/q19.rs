use crate::utils::*;

extern crate runtime;
use runtime::graph::*;
use runtime::data::*;
use runtime::operations::*;

use std::collections::HashMap;

// select
// 	sum(l_extendedprice* (1 - l_discount)) as revenue
// from
// 	lineitem,
// 	part
// where
// 	(
// 		p_partkey = l_partkey
// 		and p_brand = 'Brand#12'
// 		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
// 		and l_quantity >= 1 and l_quantity <= 1 + 10
// 		and p_size between 1 and 5
// 		and l_shipmode in ('AIR', 'AIR REG')
// 		and l_shipinstruct = 'DELIVER IN PERSON'
// 	)
// 	or
// 	(
// 		p_partkey = l_partkey
// 		and p_brand = 'Brand#23'
// 		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
// 		and l_quantity >= 10 and l_quantity <= 10 + 10
// 		and p_size between 1 and 10
// 		and l_shipmode in ('AIR', 'AIR REG')
// 		and l_shipinstruct = 'DELIVER IN PERSON'
// 	)
// 	or
// 	(
// 		p_partkey = l_partkey
// 		and p_brand = 'Brand#34'
// 		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
// 		and l_quantity >= 20 and l_quantity <= 20 + 10
// 		and p_size between 1 and 15
// 		and l_shipmode in ('AIR', 'AIR REG')
// 		and l_shipinstruct = 'DELIVER IN PERSON'
// 	);

pub fn query(tableinput: HashMap<String, TableInput>, output_reader: &mut NodeReader<ArrayRow>) -> ExecutionService<ArrayRow> {
    // CSV Reader node
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput);

    let hash_join_node = HashJoinNode::node(
        vec!["l_partkey".into()], // l_partkey on lineitem
        vec!["p_partkey".into()], // p_partkey on part
        JoinType::Inner
    );

    fn where_predicate(record: &ArrayRow) -> bool {
        // Large predicate based on lineitem and part
        (
            String::from(&record.values[18]) == "Brand#12" &&
            vec!["SM CASE".into(), "SM BOX".into(), "SM PACK".into(), "SM PKG".into()].contains(&String::from(&record.values[21])) &&
            i32::from(&record.values[4]) >= 1 &&
            i32::from(&record.values[4]) <= 1 + 10 &&
            i32::from(&record.values[20]) > 1 &&
            i32::from(&record.values[20]) < 5 &&
            vec!["AIR".into(), "AIR REG".into()].contains(&String::from(&record.values[14])) &&
            String::from(&record.values[13]) == "DELIVER IN PERSON"
        ) ||
        (
            String::from(&record.values[18]) == "Brand#23" &&
            vec!["MED BAG".into(), "MED BOX".into(), "MED PKG".into(), "MED PACK".into()].contains(&String::from(&record.values[21])) &&
            i32::from(&record.values[4]) >= 10 &&
            i32::from(&record.values[4]) <= 10 + 10 &&
            i32::from(&record.values[20]) > 1 &&
            i32::from(&record.values[20]) < 10 &&
            vec!["AIR".into(), "AIR REG".into()].contains(&String::from(&record.values[14])) &&
            String::from(&record.values[13]) == "DELIVER IN PERSON"
        ) ||
        (
            String::from(&record.values[18]) == "Brand#34" &&
            vec!["LG CASE".into(), "LG BOX".into(), "LG PACK".into(), "LG PKG".into()].contains(&String::from(&record.values[21])) &&
            i32::from(&record.values[4]) >= 20 &&
            i32::from(&record.values[4]) <= 20 + 10 &&
            i32::from(&record.values[20]) > 1 &&
            i32::from(&record.values[20]) < 15 &&
            vec!["AIR".into(), "AIR REG".into()].contains(&String::from(&record.values[14])) &&
            String::from(&record.values[13]) == "DELIVER IN PERSON"
        )
    }
    let where_node = WhereNode::node(where_predicate);

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
    hash_join_node.subscribe_to_node(&lineitem_csvreader_node,0);
    hash_join_node.subscribe_to_node(&part_csvreader_node, 1);
    where_node.subscribe_to_node(&hash_join_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&groupby_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<ArrayRow>::create();
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(hash_join_node);
    service.add(part_csvreader_node);
    service.add(lineitem_csvreader_node);
    service
}