// use polars::series::Series;
use polars::prelude::*;

use crate::{graph::ExecutionNode, processor::SetProcessorV2};
use crate::data::*;
use crate::data::Schema;

pub struct CSVReaderBuilder {
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
            delimiter: ',',
            has_headers: false,
            filtered_cols: vec![],
        }
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
        CSVReaderNode::new_with_params(self.delimiter, self.has_headers, self.filtered_cols.clone())
    }
}

pub struct CSVReaderNode;

/// A factory method for creating `ExecutionNode<Series>` that can
/// read csv files.
impl CSVReaderNode {
    pub fn node() -> ExecutionNode<Series> {
        let data_processor = CSVReader::new_with_params('|', false, vec![]);
        ExecutionNode::<Series>::from_set_processor(data_processor)
    }

    pub fn new_with_params(delimiter: char, has_headers: bool, filtered_cols: Vec<String>) -> ExecutionNode<Series> {
        let data_processor = CSVReader::new_with_params(delimiter, has_headers, filtered_cols);
        ExecutionNode::<Series>::from_set_processor(data_processor)
    }

}

/// A custom SetProcessor<Series> type for reading csv files.
struct CSVReader {
    delimiter: char,
    has_headers: bool,
    filtered_cols: Vec<String>,
}

impl SetProcessorV2<polars::prelude::Series> for CSVReader {
    // Default implementation duplicates the schema.
    fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
        if self.filtered_cols.is_empty() {
            let output_columns = input_schema.columns.clone();
            Schema::new(format!("csvreader({})",input_schema.table), output_columns)
        } else {
            let mut output_columns = Vec::new();
            for col in &input_schema.columns {
                if self.filtered_cols.contains(&col.name) {
                    output_columns.push(col.clone());
                }
            }
            Schema::new(format!("csvreader({})",input_schema.table), output_columns)
        }
    }

    /// This function receives a series of csv filenames, then read individual rows from those files
    /// and return those records (in a batch). These returned records will be sent to output channels.
    fn process_v1(&self, input_set: &DataBlock<Series>) -> DataBlock<Series> {
        let input_schema = self._get_input_schema(input_set.metadata());
        let metadata = self._build_output_metadata(input_set.metadata());
        let schema_columns: Vec<String> = input_schema.columns.iter().map(|x| x.name.clone()).collect();

        // // If filtered_cols is empty, take all columns.
        // let take_column: Vec<bool> = if self.filtered_cols.is_empty() {
        //     vec![true; record_length]
        // } else {
        //     input_schema.columns.iter().map(|x| self.filtered_cols.contains(&x.name)).collect()
        // };

        let mut all_df = vec![];
        for r in input_set.data().iter() {
            let unwrapped = r.utf8().unwrap();
            let processed_df = unwrapped.into_iter().map(|file| {
                let mut df = polars::prelude::CsvReader::from_path(file.unwrap()).unwrap()
                .has_header(self.has_headers)
                .with_delimiter(self.delimiter as u8)
                .finish().unwrap();

                if !self.has_headers {
                    df.set_column_names(&schema_columns).unwrap();
                }
                df
            }).collect::<Vec<polars::prelude::DataFrame>>();
            all_df.extend(processed_df);
        }
        let mut result_df = all_df[0].clone();
        for df in all_df.iter().skip(1) {
            result_df.vstack_mut(df).unwrap();
        }
        if result_df.should_rechunk() {
            result_df.rechunk();
        }
        let df_series: &Vec<Series> = result_df.get_columns();
        DataBlock::new(df_series.clone(), metadata)
    }
}

/// A factory method for creating the custom SetProcessor<Series> type for
/// reading csv files
impl CSVReader {
    pub fn new_with_params(delimiter: char, has_headers: bool, filtered_cols: Vec<String>) -> Box<dyn SetProcessorV2<Series>> {
        Box::new(CSVReader {delimiter, has_headers, filtered_cols})
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
        let csvreader = CSVReaderNode::new_with_params(',', true, vec![]);

        // The CSV files that we want to be read by this node => data for DataBlock.
        let input_vec = vec![
            Series::new("input_files",&["src/resources/lineitem-100.csv","src/resources/lineitem-100.csv"]),
        ];

        // Metadata for DataBlock
        let lineitem_schema = Schema::from_example("lineitem").unwrap();
        let mut metadata =  MetaCell::Schema(lineitem_schema.clone()).into_meta_map();
        *metadata.entry(DATABLOCK_TOTAL_RECORDS.to_string()).or_insert(MetaCell::Float(0.0)) = MetaCell::Float(200.0);

        // Since not using generator, change the input file structure to multiple datablocks
        let dblock1 = DataBlock::new(input_vec.clone(), metadata.clone());
        csvreader.write_to_self(0, DataMessage::from(dblock1));
        let dblock2 = DataBlock::new(input_vec, metadata);
        csvreader.write_to_self(0, DataMessage::from(dblock2));
        csvreader.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&csvreader);
        csvreader.run();

        let total_input_len = 400;
        let mut total_output_len = 0;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let data = dblock.data();
            let message_len = data[0].len();
            total_output_len += message_len;
            assert_eq!(
                dblock.metadata().get(SCHEMA_META_NAME).unwrap().to_schema().columns,
                lineitem_schema.columns
            );
        }
        // Assert total record length.
        assert_eq!(total_output_len, total_input_len);
    }
}
