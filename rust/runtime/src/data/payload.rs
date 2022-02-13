use getset::Getters;
use std::{sync::Arc, collections::HashMap};

/// Either actual data (`DataBlock`) or other special signals (e.g., EOF, Signal).
/// 
/// This is the unit of light-weight clone when exchanging messages via channels. That is,
/// `Arc` provides an efficient cloning mechanism for any arbitrary data stored in
/// `DataBlock`.
#[derive(Debug, PartialEq)]
pub enum Payload<T> {
    EOF,
    Some(Arc<DataBlock<T>>),
    Signal(Signal),
}

impl<T> Clone for Payload<T> {
    fn clone(&self) -> Self {
        match self {
            Self::EOF => Self::EOF,
            Self::Some(records) => Self::Some(records.clone()),
            Self::Signal(s) => Self::Signal(s.clone()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Signal {
    STOP,
}

impl Clone for Signal {
    fn clone(&self) -> Self {
        match self {
            Self::STOP => Self::STOP,
        }
    }
}

/// Data and metadata
/// 
/// Introduced to store the index for the primary key. 
/// TODO: Use something better than the String-String map for metadata.
#[derive(Getters, Debug, PartialEq)]
pub struct DataBlock<T> {
    #[getset(get = "pub")]
    data: Vec<T>,

    #[getset(get = "pub")]
    metadata: HashMap<String, String>,
}

impl<T> DataBlock<T> {

    /// Convenient public constructor for tests.
    pub fn from_records(records: Vec<T>) -> Self {
        DataBlock { data: records, metadata: HashMap::new() }
    }

    /// Public constructor.
    pub fn new(data: Vec<T>, metadata: HashMap<String, String>) -> Self {
        DataBlock {data, metadata}
    }
}