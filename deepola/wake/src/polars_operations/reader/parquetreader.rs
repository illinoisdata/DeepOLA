use std::fs::File;

// use polars::series::Series;
use polars::prelude::*;

use crate::data::*;
use crate::graph::ExecutionNode;
use crate::processor::StreamProcessor;

pub struct ParquetReaderBuilder {
    column_names: Option<Vec<String>>,
    projected_cols: Option<Vec<usize>>,
}

impl Default for ParquetReaderBuilder {
    fn default() -> Self {
        ParquetReaderBuilder {
            column_names: Option::None,
            projected_cols: Option::None,
        }
    }
}

impl ParquetReaderBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn column_names(&mut self, column_names: Option<Vec<String>>) -> &mut Self {
        self.column_names = column_names;
        self
    }

    pub fn projected_cols(&mut self, projected_cols: Option<Vec<usize>>) -> &mut Self {
        self.projected_cols = projected_cols;
        self
    }

    pub fn build(&self) -> ExecutionNode<DataFrame> {
        let data_processor =
            ParquetReader::new(self.column_names.clone(), self.projected_cols.clone());
        ExecutionNode::<DataFrame>::new(Box::new(data_processor), 1)
    }
}

/// A custom SetProcessor<Series> type for reading csv files.
struct ParquetReader {
    column_names: Option<Vec<String>>,
    projected_cols: Option<Vec<usize>>,
}

/// A factory method for creating the custom SetProcessor<Series> type for
/// reading csv files
impl ParquetReader {
    pub fn new(column_names: Option<Vec<String>>, projected_cols: Option<Vec<usize>>) -> Self {
        ParquetReader {
            column_names,
            projected_cols,
        }
    }

    fn dataframe_from_filename(&self, filename: &str) -> DataFrame {
        let file = File::open(filename).unwrap();
        let mut reader = polars::prelude::ParquetReader::new(file);
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

impl StreamProcessor<DataFrame> for ParquetReader {
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
                    for series in dblock.data().iter() {
                        // This must be a length-one Polars series containing
                        // file names in its rows
                        let rows = series.utf8().unwrap();

                        // each file name produces multiple Series (each is a column)
                        rows.into_iter().for_each(|filename| {
                            let df = self.dataframe_from_filename(filename.unwrap());
                            let message = DataMessage::from(DataBlock::from(df));
                            output_stream.write(message);
                        });
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
