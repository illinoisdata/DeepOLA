use crate::{graph::ExecutionNode, processor::SetProcessorV1};
use crate::data::*;
use crate::graph::*;
use rayon::prelude::*;
use itertools::izip;
use generator::{Generator, Gn};

pub struct CSVReaderBuilder {
    batch_size: usize,
    delimiter: char,
    has_headers: bool,
}

impl Default for CSVReaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CSVReaderBuilder {
    pub fn new() -> Self {
        CSVReaderBuilder {
            batch_size: 100_000,
            delimiter: ',',
            has_headers: false,
        }
    }

    pub fn batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.batch_size = batch_size;
        self
    }

    pub fn delimiter(&mut self, delimiter: char) -> &mut Self {
        self.delimiter = delimiter;
        self
    }

    pub fn has_headers(&mut self, has_header: bool) -> &mut Self {
        self.has_headers = has_header;
        self
    }

    pub fn build(&self) -> ExecutionNode<ArrayRow> {
        CSVReaderNode::new_with_params(self.batch_size, self.delimiter, self.has_headers)
    }
}

pub struct CSVReaderNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// read csv files.
impl CSVReaderNode {
    pub fn node(batch_size: usize) -> ExecutionNode<ArrayRow> {
        let data_processor = CSVReader::new_boxed(batch_size);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }

    pub fn new_with_params(batch_size: usize, delimiter: char, has_headers: bool) -> ExecutionNode<ArrayRow> {
        let data_processor = CSVReader::new_with_params(batch_size, delimiter, has_headers);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }

}

/// A custom SetProcessor<ArrayRow> type for reading csv files.
struct CSVReader {
    batch_size: usize,
    delimiter: char,
    has_headers: bool,
}

impl SetProcessorV1<ArrayRow> for CSVReader {
    /// This function receives a series of csv filenames, then read individual rows from those files
    /// and return those records (in a batch). These returned records will be sent to output channels.
    fn process_v1<'a>(&'a self, input_set: &'a DataBlock<ArrayRow>) -> Generator<'a, (), DataBlock<ArrayRow>> {
        // Each input ArrayRow contains the name of the files.
        // Each output DataBlock should contain rows of batch size.
        // The output DataBlock that you send should have this schema?
        Gn::new_scoped(
            move |mut s| {
                let input_schema = self._get_input_schema(input_set.metadata());
                let mut metadata = self._build_output_metadata(input_set.metadata());
                let input_total_records = f64::from(input_set.metadata().get(DATABLOCK_TOTAL_RECORDS).unwrap());

                let mut byte_records: Vec<csv::ByteRecord> = vec![];
                let mut total_records = 0;
                let record_length = input_schema.columns.len();
                let mut records: Vec<ArrayRow> = Vec::with_capacity(self.batch_size);
                for r in input_set.data().iter() {
                    let mut reader =
                      csv::ReaderBuilder::new()
                          .delimiter(self.delimiter as u8)
                          .has_headers(self.has_headers as bool)
                          .from_path(r.values[0].to_string())
                          .unwrap();

                    // With Byte records, UTF-8 validation is not performed.
                    let iter = reader.byte_records();
                    for result in iter {
                        let record = result.unwrap();
                        byte_records.push(record);
                        if byte_records.len() == self.batch_size {
                            byte_records.par_iter().map(|byte_record| {
                                    let mut data_cells = Vec::with_capacity(record_length);
                                    for (value,column) in izip!(byte_record,&input_schema.columns) {
                                        data_cells.push(
                                            DataCell::create_data_cell_from_bytes(
                                                value, &column.dtype).unwrap()
                                            );
                                    }
                                    ArrayRow::from_vector(data_cells)
                                }
                            ).collect_into_vec(&mut records);
                            total_records += records.len();
                            *metadata.get_mut(&DATABLOCK_CARDINALITY.to_string()).unwrap() = MetaCell::from((total_records as f64)/input_total_records);
                            let message = DataBlock::new(records, metadata.clone());
                            s.yield_(message);
                            byte_records.clear(); // Empty the byte_records.
                            records = Vec::with_capacity(self.batch_size);  // Empty the records.
                        }
                    }
                }
                if !byte_records.is_empty() {
                    byte_records.par_iter().map(|byte_record| {
                            let mut data_cells = Vec::with_capacity(record_length);
                            for (value,column) in izip!(byte_record,&input_schema.columns) {
                                data_cells.push(
                                    DataCell::create_data_cell_from_bytes(
                                        value, &column.dtype).unwrap()
                                    );
                            }
                            ArrayRow::from_vector(data_cells)
                        }
                    ).collect_into_vec(&mut records);
                    total_records += records.len();
                    *metadata.get_mut(&DATABLOCK_CARDINALITY.to_string()).unwrap() = MetaCell::from((total_records as f64)/input_total_records);
                    let message = DataBlock::new(records, metadata);
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
    pub fn new_boxed(batch_size: usize) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(CSVReader {batch_size, delimiter: ',', has_headers: true})
    }

    pub fn new_with_params(batch_size: usize, delimiter: char, has_headers: bool) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(CSVReader {batch_size, delimiter, has_headers})
    }
}

pub fn get_example_arrayrow_messages() -> Vec<DataMessage<ArrayRow>> {
    let batch_size = 50;
    let csvreader = CSVReaderNode::node(batch_size);
    // The CSV files that we want to be read by this node => data for DataBlock.
    let input_vec = vec![
        ArrayRow::from_vector(vec![DataCell::from("src/resources/lineitem-100.csv")]),
        ArrayRow::from_vector(vec![DataCell::from("src/resources/lineitem-100.csv")])
    ];
    // Metadata for DataBlock
    let lineitem_schema = Schema::from_example("lineitem").unwrap();
    let metadata = MetaCell::Schema(lineitem_schema).into_meta_map();
    let dblock = DataBlock::new(input_vec, metadata);
    csvreader.write_to_self(0, DataMessage::from(dblock));
    csvreader.write_to_self(0, DataMessage::eof());
    let reader_node = NodeReader::new(&csvreader);
    csvreader.run();
    let mut output_messages = vec![];
    loop {
        let message = reader_node.read();
        if message.is_eof() {
            break;
        }
        output_messages.push(message.clone());
    }
    output_messages
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::DataMessage;
    use crate::graph::NodeReader;

    #[test]
    fn test_csv_reader_node() {
        // Create a CSV Reader Node with lineitem Schema.
        let batch_size = 50;
        let csvreader = CSVReaderNode::node(batch_size);

        // The CSV files that we want to be read by this node => data for DataBlock.
        let input_vec = vec![
            ArrayRow::from_vector(vec![DataCell::from("src/resources/lineitem-100.csv")]),
            ArrayRow::from_vector(vec![DataCell::from("src/resources/lineitem-100.csv")])
        ];
        // Metadata for DataBlock
        let lineitem_schema = Schema::from_example("lineitem").unwrap();
        let mut metadata =  MetaCell::Schema(lineitem_schema.clone()).into_meta_map();
        *metadata.entry(DATABLOCK_TOTAL_RECORDS.to_string()).or_insert(MetaCell::Float(0.0)) = MetaCell::Float(200.0);

        let dblock = DataBlock::new(input_vec, metadata);
        csvreader.write_to_self(0, DataMessage::from(dblock));
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
                dblock.metadata().get(SCHEMA_META_NAME).unwrap(),
                &MetaCell::Schema(lineitem_schema.clone())
            );
        }
        // Assert total record length.
        assert_eq!(total_output_len, total_input_len);
        assert!(number_of_blocks <= 1 + total_input_len/batch_size);
        assert!(total_input_len/batch_size <= number_of_blocks);
    }
}