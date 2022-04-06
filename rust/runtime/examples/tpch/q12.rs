use crate::utils::TableInput;

extern crate runtime;
use runtime::graph::*;
use runtime::data::*;
use runtime::operations::*;

use std::collections::HashMap;
use std::cmp;

// select
// 	l_shipmode,
// 	sum(case
// 		when o_orderpriority = '1-URGENT'
// 			or o_orderpriority = '2-HIGH'
// 			then 1
// 		else 0
// 	end) as high_line_count,
// 	sum(case
// 		when o_orderpriority <> '1-URGENT'
// 			and o_orderpriority <> '2-HIGH'
// 			then 1
// 		else 0
// 	end) as low_line_count
// from
// 	orders,
// 	lineitem
// where
// 	o_orderkey = l_orderkey
// 	and l_shipmode in ('MAIL', 'SHIP')
// 	and l_commitdate < l_receiptdate
// 	and l_shipdate < l_commitdate
// 	and l_receiptdate >= date '1994-01-01'
// 	and l_receiptdate < date '1994-01-01' + interval '1' year
// group by
// 	l_shipmode
// order by
// 	l_shipmode;

pub fn query(tableinput: HashMap<String, TableInput>, output_reader: &mut NodeReader<ArrayRow>) -> ExecutionService<ArrayRow> {
    // Lineitem CSVReaderNode
    let lineitem_batch_size = tableinput.get(&"lineitem".to_string()).unwrap().batch_size.clone();
    let lineitem_input_files = tableinput.get(&"lineitem".to_string()).unwrap().input_files.clone();
    let lineitem_csvreader_node = CSVReaderNode::new_with_params(lineitem_batch_size, '|', false);
    let mut lineitem_file_names = vec![];
    for input_file in lineitem_input_files {
        lineitem_file_names.push(ArrayRow::from_vector(
            vec![DataCell::from(input_file)]
        ));
    }
    let lineitem_schema = Schema::from_example("lineitem").unwrap();
    let metadata = HashMap::from(
        [(SCHEMA_META_NAME.into(), MetaCell::Schema(lineitem_schema.clone()))]
    );
    let dblock = DataBlock::new(lineitem_file_names, metadata);
    lineitem_csvreader_node.write_to_self(0, DataMessage::from(dblock));
    lineitem_csvreader_node.write_to_self(0, DataMessage::eof());

    // ORDERS CSVReaderNode
    let orders_batch_size = tableinput.get(&"orders".to_string()).unwrap().batch_size.clone();
    let orders_input_files = tableinput.get(&"orders".to_string()).unwrap().input_files.clone();
    let orders_csvreader_node = CSVReaderNode::new_with_params(orders_batch_size, '|', false);
    let mut orders_file_names = vec![];
    for input_file in orders_input_files {
        orders_file_names.push(ArrayRow::from_vector(
            vec![DataCell::from(input_file)]
        ));
    }
    let orders_schema = Schema::from_example("orders").unwrap();
    let metadata = HashMap::from(
        [(SCHEMA_META_NAME.into(), MetaCell::Schema(orders_schema.clone()))]
    );
    let dblock = DataBlock::new(orders_file_names, metadata);
    orders_csvreader_node.write_to_self(0, DataMessage::from(dblock));
    orders_csvreader_node.write_to_self(0, DataMessage::eof());

    // WHERE Node
    fn predicate(record: &ArrayRow) -> bool {
        (record.values[14] == "MAIL" || record.values[14] == "SHIP") &&
        (record.values[11] < record.values[12]) &&
        (record.values[10] < record.values[11]) &&
        (record.values[12] >= DataCell::from("1994-01-01")) &&
        (record.values[12] < DataCell::from("1995-01-01"))
    }
    let where_node = WhereNode::node(predicate);

    // JOIN node
    let mut merge_join_builder = MergeJoinBuilder::new();
    merge_join_builder.left_on_index(vec![0]);
    merge_join_builder.right_on_index(vec![0]);
    let merge_join_node = merge_join_builder.build();

    // EXPRESSION Node
    fn high_line_count(record: &ArrayRow) -> DataCell {
        if record.values[20] == "1-URGENT" || record.values[20] == "2-HIGH" {
            DataCell::Integer(1)
        } else {
            DataCell::Integer(0)
        }
    }
    fn low_line_count(record: &ArrayRow) -> DataCell {
        if record.values[20] != "1-URGENT" && record.values[20] != "2-HIGH" {
            DataCell::Integer(1)
        } else {
            DataCell::Integer(0)
        }
    }
    let expressions = vec![
        Expression {
            predicate: high_line_count,
            alias: "high_line_count".into(),
            dtype: DataType::Integer
        },
        Expression {
            predicate: low_line_count,
            alias: "low_line_count".into(),
            dtype: DataType::Integer
        }
    ];
    let expression_node = ExpressionNode::node(expressions);

    // GROUP BY Aggregate Node
    let aggregates = vec![
        Aggregate {
            column: "high_line_count".into(),
            operation: AggregationOperation::Sum,
            alias: Some("high_line_count".into()),
        },
        Aggregate {
            column: "low_line_count".into(),
            operation: AggregationOperation::Sum,
            alias: Some("low_line_count".into()),
        },
    ];
    let groupby_cols = vec!["l_shipmode".into()];
    let groupby_node = GroupByNode::node(groupby_cols, aggregates);

    // SELECT node
    let selected_cols = vec!["*".into()];
    let mut select_node_builder = SelectNodeBuilder::new(selected_cols);
    fn order_by_predicate(a: &ArrayRow, b: &ArrayRow) -> cmp::Ordering {
        if a.values[0] < b.values[0] {
            cmp::Ordering::Less
        } else if a.values[0] > b.values[0] {
            cmp::Ordering::Greater
        } else {
            cmp::Ordering::Equal
        }
    }
    select_node_builder.orderby(order_by_predicate);
    let select_node = select_node_builder.build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node,0);
    merge_join_node.subscribe_to_node(&where_node,0);
    merge_join_node.subscribe_to_node(&orders_csvreader_node,1);
    expression_node.subscribe_to_node(&merge_join_node, 0);
    groupby_node.subscribe_to_node(&expression_node,0);
    select_node.subscribe_to_node(&groupby_node,0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<ArrayRow>::create();
    service.add(select_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(merge_join_node);
    service.add(where_node);
    service.add(orders_csvreader_node);
    service.add(lineitem_csvreader_node);

    service
}