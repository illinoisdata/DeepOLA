use std::{marker::PhantomData, sync::Arc};

use getset::{Getters, Setters};
use polars::prelude::DataFrame;

use crate::{processor::{MessageProcessor}, graph::ExecutionNode};


/// Factory class for generating an Appender-type execution node, which is designed to support 
/// [AppenderOp] transformation.
/// 
/// Type `P` stands for Procesor, which must be of type [AppenderOp<T>].
/// Type `T` stands for Data Type.
#[derive(Getters, Setters)]
pub struct AppenderNode<T, P> 
where
    P: AppenderOp<T>
{
    #[set = "pub"]
    appender: P,
    
    // Necessary to have T as a generic type
    phantom: PhantomData<T>,
}

impl<T, P: AppenderOp<T>> Default for AppenderNode<T, P> {
    fn default() -> Self {
        Self { appender: P::new(), phantom: PhantomData::default() }
    }
}

impl<T: 'static + Send, P> AppenderNode<T, P> 
where
    P: 'static + AppenderOp<T> + MessageProcessor<T> + Clone
{
    pub fn new() -> Self {
        AppenderNode::default()
    }

    /// Use this method to create an Appender-type node with a custom mapper.
    /// 
    /// Example:
    /// ```
    /// use wake::polars_operations::{AppenderNode, MapAppender};
    /// use polars::prelude::DataFrame;
    /// 
    /// AppenderNode::<DataFrame, MapAppender>::new().appender(
    ///     MapAppender::new( Box::new(|x: &DataFrame| x.clone() ))
    /// ).build();
    /// ```
    pub fn appender(&mut self, appender: P) -> &mut Self {
        self.appender = appender;
        self
    }

    pub fn build(&self) -> ExecutionNode<T> {
        let data_processor = self.appender.clone();
        ExecutionNode::<T>::new(Box::new(data_processor), 1)
    }
}


/// Useful for creating a simple, memoryless Appender operation such as row filtering, column
/// projection, etc. Not the best for implementing join operations because they may require
/// materialized tables.
pub trait AppenderOp<T> : Send {
    fn new() -> Self;

    fn map(&self, df: &T) -> T;
}

/// A concrete implementation of type `P`, which implements all of AppenderOp<T> + 
/// MessageProcessor<T> + Clone, for T = DataFrame.
#[derive(Clone)]
pub struct MapAppender {
    mapper: Arc<Box<dyn Fn(&DataFrame) -> DataFrame>>,
}

impl MapAppender {
    pub fn new(mapper: Box<dyn Fn(&DataFrame) -> DataFrame>) -> Self {
        MapAppender { mapper: Arc::new(mapper) }
    }
}

unsafe impl Send for MapAppender {}

impl AppenderOp<DataFrame> for MapAppender {
    fn new() -> Self {
        Self::new(Box::new(|data| data.clone()))
    }

    fn map(&self, data: &DataFrame) -> DataFrame {
        (self.mapper)(data)
    }
}

impl MessageProcessor<DataFrame> for MapAppender {
    fn process_msg(&self, input: &DataFrame) -> Option<DataFrame> {
        Some(self.map(input))
    }
}


#[cfg(test)]
mod tests {
    use crate::{graph::NodeReader, data::DataMessage};

    use super::*;
    use polars::prelude::*;

    #[test]
    fn identity_appender_node() {
        let identity = AppenderNode::<DataFrame, MapAppender>::new().build();
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
    fn filter_rows_node() {
        let row_filter = AppenderNode::<DataFrame, MapAppender>::new()
            .appender(MapAppender::new(Box::new(
                |df: &DataFrame| {
                    let a = df.column("col2").unwrap();
                    let mask = a.equal("my").unwrap();
                    df.filter(&mask).unwrap()
                }))
            )
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
    fn column_projetion_node() {
        let projector = AppenderNode::<DataFrame, MapAppender>::new()
            .appender(MapAppender::new(Box::new(|df|
                df.select(["col1"]).unwrap()
            )))
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
    fn add_new_column_node() {
        // This projector adds a new column "col3" that contains the length of the string values
        // in column "col2".
        let projector = AppenderNode::new()
            .appender(MapAppender::new(Box::new(|df| {
                    let mut col = df.column("col2").unwrap().clone();
                    col = str_to_len(&col);
                    col.rename("col3");
                    df.hstack(&[col.clone()]).unwrap()
                }))
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
