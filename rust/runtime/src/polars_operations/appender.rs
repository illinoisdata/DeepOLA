use polars::prelude::DataFrame;

use crate::{processor::StreamProcessor, data::{Payload, DataMessage}, graph::ExecutionNode};


/// Factory class for generating an Appender-type execution node, which is designed to support 
/// [AppenderOp] transformation.
pub struct Appender {
    data_map: fn(&DataFrame) -> DataFrame,
}

impl Default for Appender {
    fn default() -> Self {
        Self { data_map: |df| df.clone()  }
    }
}

impl Appender {
    pub fn new() -> Self {
        Appender::default()
    }

    pub fn map(&mut self, data_map: fn(&DataFrame) -> DataFrame) -> &mut Self {
        self.data_map = data_map;
        self
    }

    pub fn build(&self) -> ExecutionNode<DataFrame> {
        let data_processor = AppenderOp {
            data_map: self.data_map.clone()
        };
        ExecutionNode::<DataFrame>::new(Box::new(data_processor), 1)
    }
}


/// Useful for creating a simple, memoryless Appender operation such as row filtering, column
/// projection, etc. Not the best for implementing join operations because they may require
/// materialized tables.
pub struct AppenderOp {
    data_map: fn(&DataFrame) -> DataFrame,
}

unsafe impl Send for AppenderOp {}

impl StreamProcessor<DataFrame> for AppenderOp {
    fn process(
        &self,
        input_stream: crate::channel::MultiChannelReader<DataFrame>,
        output_stream: crate::channel::MultiChannelBroadcaster<DataFrame>,
    ) {
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            match message.payload() {
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Some(data_block) => {
                    let output = (self.data_map)(data_block.data());
                    let message = DataMessage::from(output);
                    output_stream.write(message);
                },
                Payload::Signal(_) => {
                    break;
                },
            }
        }
    }
}

impl From<fn(&DataFrame) -> DataFrame> for AppenderOp
{
    /// Constructor from a closure
    fn from(data_map: fn(&DataFrame) -> DataFrame) -> Self {
        AppenderOp { data_map }
    }
}


#[cfg(test)]
mod tests {
    use crate::graph::NodeReader;

    use super::*;
    use polars::prelude::*;

    #[test]
    fn identity_appender() {
        let identity = Appender::new().build();
        let input_df = df!(
            "col1" => &[
                "hello",
                "world",
            ],
            "col2" => &[
                "my",
                "name"
            ]
        ).unwrap();

        identity.write_to_self(0, DataMessage::from(input_df.clone()));
        identity.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&identity);
        identity.run();

        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let output_df = message.datablock().data();
            assert_eq!(*output_df, input_df);
        }
    }

    /// This test has an example of how to filter rows.
    #[test]
    fn filter_rows() {
        let row_filter = Appender::new()
            .map(|df| {
                let a = df.column("col2").unwrap();
                let mask = a.equal("my").unwrap();
                df.filter(&mask).unwrap()
            })
            .build();
        let input_df = df!(
            "col1" => &["hello", "world"],
            "col2" => &["my", "name"],
        ).unwrap();
        let expected_output = df!(
            "col1" => &["hello"],
            "col2" => &["my"],
        ).unwrap();

        row_filter.write_to_self(0, DataMessage::from(input_df.clone()));
        row_filter.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&row_filter);
        row_filter.run();

        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let output_df = message.datablock().data();
            assert_eq!(*output_df, expected_output);
        }
    }

    /// This test has an example of how to select a subset of columns.
    #[test]
    fn column_projetion() {
        let projector = Appender::new()
            .map(|df|
                df.select(["col1"]).unwrap()
            )
            .build();
        let input_df = df!(
            "col1" => &["hello", "world"],
            "col2" => &["my", "name"],
        ).unwrap();
        let expected_output = df!(
            "col1" => &["hello", "world"],
        ).unwrap();

        projector.write_to_self(0, DataMessage::from(input_df.clone()));
        projector.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&projector);
        projector.run();

        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let output_df = message.datablock().data();
            assert_eq!(*output_df, expected_output);
        }
    }

    /// This test has an example of how we can add new columns.
    #[test]
    fn add_new_column() {
        // This projector adds a new column "col3" that contains the length of the string values
        // in column "col2".
        let projector = Appender::new()
            .map(|df| {
                    let mut col = df.column("col2").unwrap().clone();
                    col = str_to_len(&col);
                    col.rename("col3");
                    df.hstack(&[col.clone()]).unwrap()
                }
            )
            .build();
        let input_df = df!(
            "col1" => &["hello", "world"],
            "col2" => &["my", "name"],
        ).unwrap();
        let expected_output = df!(
            "col1" => &["hello", "world"],
            "col2" => &["my", "name"],
            "col3" => &[2 as u32, 4 as u32]
        ).unwrap();

        projector.write_to_self(0, DataMessage::from(input_df.clone()));
        projector.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&projector);
        projector.run();

        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let output_df = message.datablock().data();
            assert_eq!(*output_df, expected_output);
        }
    }

    fn str_to_len(str_val: &Series) -> Series {
        str_val.utf8()
            .unwrap()
            .into_iter()
            .map(|opt_name: Option<&str>| {
                opt_name.map(|name: &str| name.len() as u32)
            })
            .collect::<UInt32Chunked>()
            .into_series()
    }

}
