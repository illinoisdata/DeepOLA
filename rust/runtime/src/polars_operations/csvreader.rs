// use polars::series::Series;
use polars::prelude::*;

use crate::processor::StreamProcessor;
use crate::{graph::ExecutionNode, processor::SetProcessorV1};
use crate::data::*;
use crate::data::Schema;
use crate::graph::*;
use generator::{Generator, Gn};

pub struct CSVReaderBuilder {
    batch_size: usize,
    delimiter: char,
    has_headers: bool,
    filtered_cols: Vec<String>
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
            filtered_cols: vec![],
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

    pub fn filtered_cols(&mut self, filtered_cols: Vec<String>) -> &mut Self {
        self.filtered_cols = filtered_cols;
        self
    }

    pub fn build(&self) -> ExecutionNode<Series> {
        CSVReaderNode::new_with_params(self.batch_size, self.delimiter, self.has_headers, self.filtered_cols.clone())
    }
}

pub struct CSVReaderNode;

/// A factory method for creating `ExecutionNode<Series>` that can
/// read csv files.
impl CSVReaderNode {
    pub fn node(batch_size: usize) -> ExecutionNode<Series> {
        let data_processor = CSVReader::new_with_params(batch_size, '|', false, vec![]);
        ExecutionNode::<Series>::new(data_processor, 1)
    }

    pub fn new_with_params(batch_size: usize, delimiter: char, has_headers: bool, filtered_cols: Vec<String>) -> ExecutionNode<Series> {
        let data_processor = CSVReader::new_with_params(batch_size, delimiter, has_headers, filtered_cols);
        ExecutionNode::<Series>::new(data_processor, 1)
    }

}

/// A custom SetProcessor<Series> type for reading csv files.
struct CSVReader {
    batch_size: usize,
    delimiter: char,
    has_headers: bool,
    filtered_cols: Vec<String>,
}

impl StreamProcessor<Series> for CSVReader {
    fn process(
        &self,
        input_stream: crate::channel::MultiChannelReader<Series>,
        output_stream: crate::channel::MultiChannelBroadcaster<Series>,
    ) {
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            match message.payload() {
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Signal(_) => break,
                Payload::Some(dblock) => {
                    output_stream.write(message);
                }
            }
        }
    }
}

// impl SetProcessorV1<Series> for CSVReader {
//     // Default implementation duplicates the schema.
//     fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
//         if self.filtered_cols.is_empty() {
//             let output_columns = input_schema.columns.clone();
//             Schema::new(format!("csvreader({})",input_schema.table), output_columns)
//         } else {
//             let mut output_columns = Vec::new();
//             for col in &input_schema.columns {
//                 if self.filtered_cols.contains(&col.name) {
//                     output_columns.push(col.clone());
//                 }
//             }
//             Schema::new(format!("csvreader({})",input_schema.table), output_columns)
//         }
//     }

//     /// This function receives a series of csv filenames, then read individual rows from those files
//     /// and return those records (in a batch). These returned records will be sent to output channels.
//     fn process_v1<'a>(&'a self, input_set: &'a DataBlock<Series>) -> Generator<'a, (), DataBlock<Series>> {
//         // Each input Series contains the name of the files.
//         // Each output DataBlock should contain rows of batch size.
//         // The output DataBlock that you send should have this schema?
//         Gn::new_scoped(
//             move |mut s| {
//                 // let input_schema = self._get_input_schema(input_set.metadata());
//                 // let mut metadata = self._build_output_metadata(input_set.metadata());
//                 // let record_length = input_schema.columns.len();
//                 // let input_total_records = f64::from(input_set.metadata().get(DATABLOCK_TOTAL_RECORDS).unwrap());

//                 // let schema_columns: Vec<String> = input_schema.columns.iter().map(|x| x.name.clone()).collect();

//                 // If filtered_cols is empty, take all columns.
//                 // let take_column: Vec<bool> = if self.filtered_cols.is_empty() {
//                 //     vec![true; record_length]
//                 // } else {
//                 //     input_schema.columns.iter().map(|x| self.filtered_cols.contains(&x.name)).collect()
//                 // };

//                 // let mut byte_records: Vec<csv::ByteRecord> = vec![];
//                 // let mut total_records = 0;
//                 // let mut records: Vec<Series> = Vec::with_capacity(self.batch_size);

//                 let data = input_set.data();
//                 println!("DATA FROM DATABLOCK: {:?}", data);

//                 // for r in input_set.data().iter() {
//                 //     // println!("ITERATING DATA");
//                 //     // let unwrapped = r.into_iter().map(|file| {
//                 //     //     let mut df = polars::prelude::CsvReader::from_path(file).unwrap()
//                 //     //     .has_header(self.has_headers)
//                 //     //     .with_delimiter(self.delimiter as u8)
//                 //     //     .finish().unwrap();
//                 //     //     df.set_column_names(&schema_columns).unwrap();
//                 //     //     *metadata.get_mut(&DATABLOCK_CARDINALITY.to_string()).unwrap() = MetaCell::from((total_records as f64)/input_total_records);
//                 //     //     let message = DataBlock::new(records.clone(), metadata.clone());
//                 //     //     s.yield_(message);
//                 //     // });
//                 // }
//                 done!();
//             }
//         )
//     }
// }

/// A factory method for creating the custom SetProcessor<Series> type for
/// reading csv files
impl CSVReader {
    pub fn new_with_params(batch_size: usize, delimiter: char, has_headers: bool, filtered_cols: Vec<String>) -> Box<dyn StreamProcessor<Series>> {
        Box::new(CSVReader {batch_size, delimiter, has_headers, filtered_cols})
    }
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
        let csvreader = CSVReaderNode::new_with_params(batch_size, ',', true, vec![]);

        // The CSV files that we want to be read by this node => data for DataBlock.
        let input_vec = vec![
            Series::new("input_files",&["src/resources/lineitem-100.csv","src/resources/lineitem-100.csv"]),
            Series::new("input_files",&["src/resources/lineitem-100.csv","src/resources/lineitem-100.csv","src/resources/lineitem-100.csv","src/resources/lineitem-100.csv"])
        ];

        println!("INPUT DATA SIZE: {}", input_vec.len());

        for element in input_vec.iter() {
            println!("RUNNING ON VECTOR ELEMENT: {:?}", element);
            // element.utf8().into_iter().map(|file_option| {
            //     println!("Row: {:?}", file_option);
            // });
        }

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
                dblock.metadata().get(SCHEMA_META_NAME).unwrap().to_schema().columns,
                lineitem_schema.columns
            );
        }
        // Assert total record length.
        assert_eq!(total_output_len, total_input_len);
        assert!(number_of_blocks <= 1 + total_input_len/batch_size);
        assert!(total_input_len/batch_size <= number_of_blocks);
    }
}
