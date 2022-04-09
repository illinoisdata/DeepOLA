use crate::utils::*;

extern crate runtime;
use runtime::graph::*;
use runtime::data::*;
use runtime::operations::*;

use std::collections::HashMap;
use std::cmp;

/// This node implements the following SQL query
// select
// 	l_returnflag,
// 	l_linestatus,
// 	sum(l_quantity) as sum_qty,
// 	sum(l_extendedprice) as sum_base_price,
// 	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
// 	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
// 	avg(l_quantity) as avg_qty,
// 	avg(l_extendedprice) as avg_price,
// 	avg(l_discount) as avg_disc,
// 	count(*) as count_order
// from
// 	lineitem
// where
// 	l_shipdate <= date '1998-12-01' - interval '90' day
// group by
// 	l_returnflag,
// 	l_linestatus
// order by
// 	l_returnflag,
// 	l_linestatus;
// limit -1;

pub fn query(tableinput: HashMap<String, TableInput>, output_reader: &mut NodeReader<ArrayRow>) -> ExecutionService<ArrayRow> {
    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput);

    // WHERE Node
    fn predicate(record: &ArrayRow) -> bool {
        // 10th index is l_shipdate
        record.values[10] <= DataCell::from("1998-09-01")
    }
    let where_node = WhereNode::node(predicate);

    // EXPRESSION Node
    fn disc_price_predicate(record: &ArrayRow) -> DataCell {
        DataCell::from(record.values[5].clone() * (DataCell::from(1) - record.values[6].clone()))
    }
    fn charge_predicate(record: &ArrayRow) -> DataCell {
        DataCell::from(record.values[5].clone() * (DataCell::from(1) - record.values[6].clone()) * (DataCell::from(1) + record.values[7].clone()))
    }
    let expressions = vec![
        Expression {
            predicate: disc_price_predicate,
            alias: "disc_price".into(),
            dtype: DataType::Float
        },
        Expression {
            predicate: charge_predicate,
            alias: "charge".into(),
            dtype: DataType::Float
        }
    ];
    let expression_node = ExpressionNode::node(expressions);

    // GROUP BY Aggregate Node
    let aggregates = vec![
        Aggregate {
            column: "l_quantity".into(),
            operation: AggregationOperation::Sum,
            alias: Some("sum_qty".into()),
        },
        Aggregate {
            column: "l_extendedprice".into(),
            operation: AggregationOperation::Sum,
            alias: Some("sum_base_price".into()),
        },
        Aggregate {
            column: "disc_price".to_string(),
            operation: AggregationOperation::Sum,
            alias: Some("sum_disc_price".into()),
        },
        Aggregate {
            column: "charge".to_string(),
            operation: AggregationOperation::Sum,
            alias: Some("sum_charge".into()),
        },
        Aggregate {
            column: "l_quantity".to_string(),
            operation: AggregationOperation::Avg,
            alias: Some("avg_qty".into()),
        },
        Aggregate {
            column: "l_extendedprice".to_string(),
            operation: AggregationOperation::Avg,
            alias: Some("avg_price".into()),
        },
        Aggregate {
            column: "l_discount".to_string(),
            operation: AggregationOperation::Avg,
            alias: Some("avg_disc".into()),
        },
        Aggregate {
            column: "l_extendedprice".to_string(),
            operation: AggregationOperation::Count,
            alias: Some("count_order".into()),
        },
    ];
    let groupby_cols = vec!["l_returnflag".into(),"l_linestatus".into()];
    let groupby_node = GroupByNode::node(groupby_cols, aggregates);

    // SELECT and ORDERBY Node
    let selected_cols = vec!["*".into()];
    let mut select_node_builder = SelectNodeBuilder::new(selected_cols);
    fn order_by_predicate(a: &ArrayRow, b: &ArrayRow) -> cmp::Ordering {
        if a.values[0] < b.values[0] {
            cmp::Ordering::Less
        } else if a.values[0] > b.values[0] {
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
    select_node_builder.orderby(order_by_predicate);
    let select_node = select_node_builder.build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node,0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node,0);
    select_node.subscribe_to_node(&groupby_node,0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<ArrayRow>::create();
    service.add(select_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(lineitem_csvreader_node);

    service
}