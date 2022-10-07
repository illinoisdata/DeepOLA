use polars::prelude::*;

use crate::data::*;
use crate::graph::ExecutionNode;
use crate::processor::StreamProcessor;

pub struct CSVReaderBuilder {
    delimiter: char,
    has_headers: bool,
    parse_dates: bool,
    column_names: Option<Vec<String>>,
    projected_cols: Option<Vec<usize>>,
}

impl Default for CSVReaderBuilder {
    fn default() -> Self {
        CSVReaderBuilder {
            delimiter: ',',
            has_headers: false,
            parse_dates: true,
            column_names: Option::None,
            projected_cols: Option::None,
        }
    }
}

impl CSVReaderBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn delimiter(&mut self, delimiter: char) -> &mut Self {
        self.delimiter = delimiter;
        self
    }

    pub fn has_headers(&mut self, has_header: bool) -> &mut Self {
        self.has_headers = has_header;
        self
    }

    pub fn column_names(&mut self, column_names: Option<Vec<String>>) -> &mut Self {
        self.column_names = column_names;
        self
    }

    pub fn projected_cols(&mut self, projected_cols: Option<Vec<usize>>) -> &mut Self {
        self.projected_cols = projected_cols;
        self
    }

    pub fn parse_dates(&mut self, parse_dates: bool) -> &mut Self {
        self.parse_dates = parse_dates;
        self
    }

    pub fn build(&self) -> ExecutionNode<DataFrame> {
        let data_processor = CSVReader::new(
            self.delimiter,
            self.has_headers,
            self.parse_dates,
            self.column_names.clone(),
            self.projected_cols.clone(),
        );
        ExecutionNode::<DataFrame>::new(Box::new(data_processor), 1)
    }
}

/// A custom SetProcessor<Series> type for reading csv files.
struct CSVReader {
    delimiter: char,
    has_headers: bool,
    parse_dates: bool,
    column_names: Option<Vec<String>>,
    projected_cols: Option<Vec<usize>>,
}

/// A factory method for creating the custom SetProcessor<Series> type for
/// reading csv files
impl CSVReader {
    pub fn new(
        delimiter: char,
        has_headers: bool,
        parse_dates: bool,
        column_names: Option<Vec<String>>,
        projected_cols: Option<Vec<usize>>,
    ) -> Self {
        CSVReader {
            delimiter,
            has_headers,
            parse_dates,
            column_names,
            projected_cols,
        }
    }

    fn dataframe_from_filename(&self, filename: &str) -> DataFrame {
        let mut reader = polars::prelude::CsvReader::from_path(filename)
            .unwrap()
            .has_header(self.has_headers)
            .with_parse_dates(self.parse_dates)
            .with_delimiter(self.delimiter as u8);
        if self.projected_cols.is_some() {
            reader = reader.with_projection(self.projected_cols.clone());
        }
        let mut df = reader.finish().unwrap();
        if self.column_names.is_some() {
            if let Some(a) = &self.column_names {
                df.set_column_names(a).unwrap();
            }
        }
        df
    }
}

impl StreamProcessor<DataFrame> for CSVReader {
    fn process_stream(
        &self,
        input_stream: crate::channel::MultiChannelReader<DataFrame>,
        output_stream: crate::channel::MultiChannelBroadcaster<DataFrame>,
    ) {
        let mut start_time = std::time::Instant::now();
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            log::info!(
                "[logging] type=execution thread={:?} action=read time={:?}",
                std::thread::current().id(),
                start_time.elapsed().as_micros()
            );
            start_time = std::time::Instant::now();
            match message.payload() {
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Signal(_) => break,
                Payload::Some(dblock) => {
                    let mut metadata = dblock.metadata().clone();
                    let expected_total_records =
                        if let Some(count) = metadata.get(DATABLOCK_TOTAL_RECORDS) {
                            f64::from(count)
                        } else {
                            log::warn!("Missing {} in metadata", DATABLOCK_TOTAL_RECORDS);
                            1.0
                        };
                    let mut currect_total_records = 0.0;
                    for series in dblock.data().iter() {
                        // This must be a length-one Polars series containing
                        // file names in its rows
                        let rows = series.utf8().unwrap();

                        // each file name produces multiple Series (each is a column)
                        for filename in rows {
                            let output_df = self.dataframe_from_filename(filename.unwrap());

                            // Update record count
                            currect_total_records += output_df.height() as f64;
                            if let Some(cardinality) = metadata.get_mut(DATABLOCK_CARDINALITY) {
                                *cardinality = MetaCell::from(f64::min(1.0,
                                    currect_total_records / expected_total_records));
                            }

                            // Compose and write output message
                            let output_dblock = DataBlock::new(output_df, metadata.clone());
                            let output_message = DataMessage::from(output_dblock);
                            output_stream.write(output_message);
                        }
                    }
                }
            }
            log::info!(
                "[logging] type=execution thread={:?} action=process time={:?}",
                std::thread::current().id(),
                start_time.elapsed().as_micros()
            );
            start_time = std::time::Instant::now();
        }
        log::info!(
            "[logging] type=execution thread={:?} action=process time={:?}",
            std::thread::current().id(),
            start_time.elapsed().as_micros()
        );
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
        let csvreader = CSVReaderBuilder::new()
            .delimiter(',')
            .has_headers(true)
            .build();

        // The CSV files that we want to be read by this node => data for DataBlock.
        let input_files = df!(
            "col" => &[
                "resources/tpc-h/data/lineitem-100.csv",
                "resources/tpc-h/data/lineitem-100.csv",
            ]
        )
        .unwrap();

        // Since not using generator, change the input file structure to multiple datablocks
        csvreader.write_to_self(0, DataMessage::from(input_files.clone()));
        csvreader.write_to_self(0, DataMessage::from(input_files.clone()));
        csvreader.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&csvreader);
        csvreader.run();

        // We are reading lineitem-100 four times.
        let total_input_len = 400;
        let mut total_output_len = 0;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let data = dblock.data();
            let message_len = data.height();
            total_output_len += message_len;
        }
        // Assert total record length.
        assert_eq!(total_output_len, total_input_len);
    }

    #[test]
    fn test_partial_col_read() {
        let input_files = df!(
            "col" => &[
                "resources/tpc-h/data/lineitem-100.csv",
            ]
        )
        .unwrap();

        // First, test a regular reader; which must read all the 16 columns for the lineitem table
        let csvreader = CSVReaderBuilder::new()
            .delimiter(',')
            .has_headers(true)
            .build();
        csvreader.write_to_self(0, DataMessage::from(input_files.clone()));
        csvreader.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&csvreader);
        csvreader.run();
        let total_column_count = 16;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let data = dblock.data();
            assert_eq!(data.width(), total_column_count);
        }

        // Second, test a column-projection reader; which must read only a subset of columns
        // as specified in the configuration
        let csvreader = CSVReaderBuilder::new()
            .delimiter(',')
            .has_headers(true)
            .projected_cols(Some(vec![0, 1, 2]))
            .build();
        csvreader.write_to_self(0, DataMessage::from(input_files.clone()));
        csvreader.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&csvreader);
        csvreader.run();
        let total_column_count = 3;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let data = dblock.data();
            println!("{:?}", data);
            assert_eq!(
                data.get_column_names(),
                vec!["l_orderkey", "l_partkey", "l_suppkey"]
            );
            assert_eq!(data.width(), total_column_count);
        }
    }
}
