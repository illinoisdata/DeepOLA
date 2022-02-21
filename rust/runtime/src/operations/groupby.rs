use super::*;
use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};
use std::{collections::HashMap};

pub struct GroupByNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// perform GROUP BY operation.
impl GroupByNode {
    pub fn new(groupby_cols: Vec<String>, aggregates: Vec<Aggregate>) -> ExecutionNode<ArrayRow> {
        let data_processor = GroupByMapper::new(groupby_cols, aggregates);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }
}

pub struct GroupByMapper {
    groupby_cols: Vec<String>,
    aggregates: Vec<Aggregate>,
}

impl GroupByMapper {
    fn _build_output_schema(&self, input_schema: Schema) -> Schema {
        let mut output_columns = Vec::new();
        for groupby_col in &self.groupby_cols {
            output_columns.push(input_schema.get_column(groupby_col.clone()));
        }
        for aggregate in &self.aggregates {
            output_columns.push(input_schema.get_column(aggregate.column.columns.clone()));
        }
        Schema::new("temp".to_string(),output_columns)
    }

    pub fn new(groupby_cols: Vec<String>, aggregates: Vec<Aggregate>) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(GroupByMapper {groupby_cols, aggregates})
    }
}

impl SetProcessorV1<ArrayRow> for GroupByMapper {
    fn process_v1<'a>(&'a self, input_set: &'a DataBlock<ArrayRow>) -> Generator<'a, (), DataBlock<ArrayRow>> {
        // Each input ArrayRow contains the name of the files.
        // Each output DataBlock should contain rows of batch size.
        // The output DataBlock that you send should have this schema?
        Gn::new_scoped(
            move |mut s| {
                let input_schema = input_set.metadata().get("schema").unwrap().to_schema();
                let metadata: HashMap<String, DataCell> = HashMap::from(
                    [("schema".into(), DataCell::Schema(self._build_output_schema(input_schema.clone())))]
                );

                let records: Vec<ArrayRow> = vec![];
                let message = DataBlock::new(records, metadata.clone());
                s.yield_(message);
                done!();
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;
    use super::{Aggregate,AggregationOperation,Expression,GroupByMapper};

    #[test]
    fn can_get_output_schema() {
        let input_schema = Schema::from_example("lineitem").unwrap();
        let record_map = Box::new(|a: &AggregationOperation| {Some(AggregationOperation::Sum)});
        let groupby_cols = vec!["l_orderkey","l_partkey"];
        let aggregates = vec![Aggregate{column: Expression{columns:"l_quantity".to_string()}, operation:AggregationOperation::Sum, alias: "sum_l_quantity".to_string()}];
        let groupby_mapper = GroupByMapper::new(
            record_map,
            input_schema,
            groupby_cols.clone(),
            aggregates.clone()
        );
        let output_schema = groupby_mapper.output_schema.unwrap();
        println!("{:?}",output_schema);
        assert_eq!(output_schema.columns.len(),groupby_cols.len() + aggregates.len());
    }

    fn test_csv_reader_node() {
        // Create a CSV Reader Node with lineitem Schema.
        let batch_size = 50;
        let csvreader = CSVReaderNode::new(batch_size);

        // The CSV files that we want to be read by this node => data for DataBlock.
        let input_vec = vec![
            ArrayRow::from_vector(vec![DataCell::Text("src/resources/lineitem-100.csv".to_string())]),
            ArrayRow::from_vector(vec![DataCell::Text("src/resources/lineitem-100.csv".to_string())])
        ];
        // Metadata for DataBlock
        let lineitem_schema = Schema::from_example("lineitem").unwrap();
        let metadata: HashMap<String, DataCell> = HashMap::from(
            [("schema".into(), DataCell::Schema(lineitem_schema.clone()))]
        );
        let dblock = DataBlock::new(input_vec,metadata);
        csvreader.write_to_self(0, DataMessage::from_data_block(dblock));
        csvreader.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&csvreader);
        csvreader.run();

        let total_input_len = 200;
        let mut total_output_len = 0;
        let mut number_of_blocks = 0;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let data = dblock.data();
            let message_len = data.len();
            number_of_blocks += 1;
            // Assert individual data block length.
            assert!(message_len <= batch_size);
            total_output_len += message_len;
            assert_eq!(
                dblock.metadata().get("schema").unwrap(),
                &DataCell::Schema(lineitem_schema.clone())
            );
        }
        // Assert total record length.
        assert_eq!(total_output_len, total_input_len);
        assert!(number_of_blocks <= 1 + total_input_len/batch_size);
        assert!(total_input_len/batch_size <= number_of_blocks);
    }
}