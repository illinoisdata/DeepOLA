use crate::graph::node::ExecutionNode;
use crate::processor::SetProcessor;
use crate::data::payload::DataBlock;
use crate::data::schema::Schema;
use crate::data::array_row::ArrayRow;
use crate::data::data_type::DataCell;
use std::{collections::HashMap};

use itertools::izip;
use generator::{Generator, Gn};

pub struct CSVReaderNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// read csv files.
impl CSVReaderNode {
    pub fn new(batch_size: usize) -> ExecutionNode<ArrayRow> {
        let data_processor = CSVReader::new(batch_size);
        ExecutionNode::<ArrayRow>::create_with_set_processor(data_processor)
    }
}

/// A custom SetProcessor<ArrayRow> type for reading csv files.
struct CSVReader {
    batch_size: usize,
}

impl SetProcessor<ArrayRow> for CSVReader {
    /// This function receives a series of csv filenames, then read individual rows from those files
    /// and return those records (in a batch). These returned records will be sent to output channels.
    fn process<'a>(&'a self, input_set: &'a DataBlock<ArrayRow>) -> Generator<'a, (), DataBlock<ArrayRow>> {
        // Each input ArrayRow contains the name of the files.
        // Each output DataBlock should contain rows of batch size.
        // The output DataBlock that you send should have this schema?
        Gn::new_scoped(
            move |mut s| {
                let input_schema = input_set.metadata().get("schema").unwrap().to_schema();
                let metadata: HashMap<String, DataCell> = HashMap::from(
                    [("schema".into(), DataCell::Schema(self._build_output_schema(input_schema)))]
                );

                let mut records: Vec<ArrayRow> = vec![];
                for r in input_set.data().iter() {
                    let mut reader = csv::Reader::from_path(r.values[0].to_string()).unwrap();
                    // Currently assumes that the first row corresponds to header.
                    // Can add a boolean header and optionally read the first row as header or data.
                    for result in reader.records() {
                        let record = result.unwrap();
                        let mut data_cells = Vec::new();
                        for (value,column) in izip!(&record,&input_schema.columns) {
                            data_cells.push(DataCell::create_data_cell(value.to_string(), &column.dtype).unwrap());
                        }
                        records.push(ArrayRow::from_vector(data_cells));

                        if records.len() == self.batch_size {
                            let message = DataBlock::new(records, metadata.clone());
                            records = vec![];
                            s.yield_(message);
                        }
                    }
                }
                if !records.is_empty() {
                    let message = DataBlock::new(records, metadata.clone());
                    s.yield_(message);
                }
                done!();
            }
        )
    }
}

/// A factory method for creating the custom SetProcessor<ArrayRow> type for
/// reading csv files
impl CSVReader {
    fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
        input_schema.clone()
    }

    pub fn new(batch_size: usize) -> Box<dyn SetProcessor<ArrayRow>> {
        Box::new(CSVReader {batch_size})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::message::DataMessage;
    use crate::graph::node::NodeReader;

    #[test]
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
        csvreader.write_to_self(DataMessage::from_data_block(dblock));
        let reader_node = NodeReader::create(&csvreader);
        csvreader.process_payload();

        let total_input_len = 200;
        let mut total_output_len = 0;
        let mut number_of_blocks = 0;
        loop {
            let message = reader_node.read();
            if message.is_some() {
                let message_len = message.clone().unwrap().len();
                number_of_blocks += 1;
                // Assert individual data block length.
                assert!(message_len <= batch_size);
                total_output_len += message_len;
                assert_eq!(
                    message.unwrap().datablock().metadata().get("schema").unwrap(),
                    &DataCell::Schema(lineitem_schema.clone())
                );
            }
            else {
                break;
            }
        }
        // Assert total record length.
        assert_eq!(total_output_len, total_input_len);
        assert!(number_of_blocks <= 1 + total_input_len/batch_size);
        assert!(total_input_len/batch_size <= number_of_blocks);
    }
}