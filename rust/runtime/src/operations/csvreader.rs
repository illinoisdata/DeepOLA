use crate::graph::node::ExecutionNode;
use crate::processor::SetProcessor;
use crate::data::payload::DataBlock;
use crate::data::schema::Schema;
use crate::data::array_row::ArrayRow;
use crate::data::data_type::DataCell;

use std::error::Error;
use itertools::izip;

pub struct CSVReaderNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// read csv files.
impl CSVReaderNode {
    pub fn new(batch_size: usize, schema: Schema) -> ExecutionNode<ArrayRow> {
        let data_processor = CSVReader::new(batch_size, schema);
        ExecutionNode::<ArrayRow>::create_with_set_processor(data_processor)
    }
}

/// A custom SetProcessor<ArrayRow> type for reading csv files.
struct CSVReader {
    input_schema: Schema,
    output_schema: Option<Schema>,
    // Currently batch_size is unused. 
    // To use the batch_size to break down a set of batch_size records in one DataBlock
    // process() would need to return a Vec<DataBlock>
    batch_size: usize,
}

impl SetProcessor<ArrayRow> for CSVReader {
    /// This function receives a series of csv filenames, then read individual rows from those files
    /// and return those records (in a batch). These returned records will be sent to output channels.
    fn process(&self, input_set: &DataBlock<ArrayRow>) -> Vec<DataBlock<ArrayRow>> {
        // Each input ArrayRow contains the name of the files.
        // Each output DataBlock should contain rows of batch size.
        // The output DataBlock that you send should have this schema?

        let mut records: Vec<ArrayRow> = vec![];
        let mut datablocks: Vec<DataBlock<ArrayRow>> = vec![];

        for r in input_set.data().iter() {
            let mut reader = csv::Reader::from_path(r.values[0].to_string()).unwrap();
            // Currently assumes that the first row corresponds to header.
            // Can add a boolean header and optionally read the first row as header or data.
            for result in reader.records() {
                let record = result.unwrap();
                let mut data_cells = Vec::new();
                for (value,column) in izip!(&record,&self.input_schema.columns) {
                    data_cells.push(DataCell::create_data_cell(value.to_string(), &column.dtype).unwrap());
                }
                records.push(ArrayRow::from_vector(data_cells));

                if records.len() == self.batch_size {
                    let datablock = DataBlock::from_records(records);
                    records = vec![];
                    datablocks.push(datablock);
                }
            }
        }
        if records.len() != 0 {
            let datablock = DataBlock::from_records(records);
            datablocks.push(datablock);
        }
        datablocks
    }
}

/// A factory method for creating the custom SetProcessor<ArrayRow> type for 
/// reading csv files
impl CSVReader {
    fn _build_output_schema(&self) -> Option<Schema> {
        Some(self.input_schema.clone())
    }

    pub fn new(batch_size: usize, schema:Schema) -> Box<dyn SetProcessor<ArrayRow>> {
        let mut csv_reader = CSVReader {
            input_schema: schema,
            output_schema: None,
            batch_size: batch_size
        };
        csv_reader.output_schema = csv_reader._build_output_schema();
        Box::new(csv_reader)
    }
}

#[cfg(test)]
mod tests {
    use super::CSVReaderNode;
    use crate::data::schema::Schema;
    use crate::data::data_type::DataCell;
    use crate::data::array_row::ArrayRow;
    use crate::data::message::DataMessage;
    use crate::graph::node::NodeReader;

    #[test]
    fn test_csv_reader_node() {
        let batch_size = 32;
        let schema = Schema::from_example("lineitem").unwrap();
        // Create a CSV Reader Node with lineitem Schema.
        let csvreader = CSVReaderNode::new(batch_size, schema);

        // The CSV files that we want to be read by this node.
        let input_vec = vec![
            ArrayRow::from_vector(vec![DataCell::Text("src/resources/lineitem-100.csv".to_string())]), 
            ArrayRow::from_vector(vec![DataCell::Text("src/resources/lineitem-100.csv".to_string())])
        ];
        csvreader.write_to_self(DataMessage::from_set(input_vec));
        let reader_node = NodeReader::create(&csvreader);
        csvreader.process_payload();

        let total_input_len = 198;
        let mut total_output_len = 0;
        let mut number_of_blocks = 0;
        loop {
            let message = reader_node.read();
            if message.is_some() {
                let message_len = message.unwrap().len();
                number_of_blocks += 1;
                // Assert individual data block length.
                assert!(message_len <= batch_size);
                total_output_len += message_len;
            }
            else {
                break;
            }
        }
        // Assert total record length.
        assert_eq!(total_output_len, total_input_len);
        assert!(number_of_blocks <= 1 + total_input_len/batch_size);
    }
}