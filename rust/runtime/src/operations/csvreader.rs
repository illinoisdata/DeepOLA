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
    fn process(&self, input_set: &DataBlock<ArrayRow>) -> DataBlock<ArrayRow> {
        // Each input ArrayRow contains the name of the files.
        // Each output DataBlock should contain rows of batch size.
        // The output DataBlock that you send should have this schema?
        let mut records: Vec<ArrayRow> = vec![];
        for r in input_set.data().iter() {
            let mut csv_records = self.from_csv(r.values[0].to_string()).unwrap();
            records.append(&mut csv_records);
        }
        DataBlock::from_records(records)
    }
}

/// A factory method for creating the custom SetProcessor<ArrayRow> type for 
/// reading csv files
impl CSVReader {
    fn _build_output_schema(&self) -> Option<Schema> {
        Some(self.input_schema.clone())
    }

    // Currently assumes that the first row corresponds to header.
    // Can add a boolean header and optionally read the first row as header or data.
    fn from_csv(&self, filename: String) -> Result<Vec<ArrayRow>, Box<dyn Error> > {
        let mut reader = csv::Reader::from_path(filename)?;
        let mut records = Vec::new();
        for result in reader.records() {
            let record = result?;
            let mut data_cells = Vec::new();
            for (value,column) in izip!(&record,&self.input_schema.columns) {
                data_cells.push(DataCell::create_data_cell(value.to_string(), &column.dtype).unwrap());
            }
            records.push(ArrayRow::from_vector(data_cells));
        }
        Ok(records)
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
        let batch_size = 1024;
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

        // Message contains a dataframe that has all the records from the input_vec CSV files.
        let message = reader_node.read().unwrap();
        assert_eq!(message.len(),198); // 99+99
    }
}