extern crate runtime;
use runtime::graph::*;
use runtime::data::{SCHEMA_META_NAME,ArrayRow,DataCell,MetaCell,Schema,DataBlock,DataMessage,DataType};
use runtime::operations::{CSVReaderNode,WhereNode,ExpressionNode,GroupByNode,SelectNodeBuilder,Expression};
use runtime::operations::{AggregationOperation, Aggregate};
use std::collections::HashMap;
use std::time::Instant;
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

pub struct TpchQ1 {
    output_reader: Option<NodeReader<ArrayRow>>
}

impl TpchQ1 {
    fn new() -> Self {
        TpchQ1 {
            output_reader: None
        }
    }

    // Service Parameters:
    // batch_size
    // CSV file names
    fn service(&mut self, batch_size: usize, file_names: Vec<String>) -> ExecutionService<ArrayRow> {
        // CSVReaderNode
        let lineitem_csvreader_node = CSVReaderNode::new_with_params(batch_size, '|', false);
        let mut input_files = vec![];
        for file_name in file_names {
            input_files.push(ArrayRow::from_vector(
                vec![DataCell::from(file_name)]
            ));
        }
        let lineitem_schema = Schema::from_example("lineitem").unwrap();
        let metadata = HashMap::from(
            [(SCHEMA_META_NAME.into(), MetaCell::Schema(lineitem_schema.clone()))]
        );
        let dblock = DataBlock::new(input_files, metadata);
        lineitem_csvreader_node.write_to_self(0, DataMessage::from(dblock));
        lineitem_csvreader_node.write_to_self(0, DataMessage::eof());

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
        // let selected_cols = vec![
        //     "l_returnflag".into(),"l_linestatus".into(),"sum_qty".into(),
        //     "sum_base_price".into(),"sum_disc_price".into(), "sum_charge".into(),
        //     "avg_qty".into(), "avg_price".into(),"avg_disc".into(),"count_order".into()
        // ];
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

        // Define the output reader node
        self.output_reader = Some(NodeReader::new(&select_node));

        // Add all the nodes to the service
        let mut service = ExecutionService::<ArrayRow>::create();
        service.add(select_node);
        service.add(groupby_node);
        service.add(expression_node);
        service.add(where_node);
        service.add(lineitem_csvreader_node);
        service
    }
}

fn main() {
    let start_time = Instant::now();
    let batch_size = 100_000;
    let file_names = vec![
        "src/resources/tpc-h/lineitem_1M.tbl".into()
    ];
    let mut query = TpchQ1::new();
    let mut q1_service = query.service(batch_size, file_names);
    q1_service.run();
    let output_reader = query.output_reader.unwrap();
    let mut result = DataBlock::from(vec![]);
    loop {
        let message = output_reader.read();
        if message.is_eof() {
            break;
        }
        let data = message.datablock();
        result = data.clone();
    }
    q1_service.join();
    let end_time = Instant::now();
    println!("Query Result");
    println!("{}", result);
    println!("Query took {:.2?}", end_time - start_time);
}