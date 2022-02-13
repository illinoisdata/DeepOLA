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

impl<T> Payload<T> {
    pub fn new(dblock: DataBlock<T>) -> Self {
        Payload::Some(Arc::new(dblock))
    }
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap};

    use super::{DataBlock, Payload};

    /// Simple test of a factory method.
    #[test]
    fn datablock_new() {
        let data: Vec<i64> = vec![19241];
        let metadata: HashMap<String, String> = HashMap::from([("key".into(), "value".into())]);
        let dblock = DataBlock::new(data.clone(), metadata.clone());
        assert_eq!(dblock.data, data);
        assert_eq!(dblock.metadata, metadata);
    }

    /// Even if a payload is cloned, their underlying data objects are the same.
    #[test]
    fn clone_doenst_copy_data() {
        let data: Vec<i64> = vec![19241];
        let metadata: HashMap<String, String> = HashMap::from([("key".into(), "value".into())]);
        let dblock = DataBlock::new(data.clone(), metadata.clone());
        let payload = Payload::new(dblock);
        let payload_clone = payload.clone();

        if let Payload::Some(dblock_arc) = payload {
            if let Payload::Some(dblock_arc2) = payload_clone {
                let data1 = dblock_arc.as_ref();
                let data2 = dblock_arc2.as_ref();
                assert!(std::ptr::eq(data1, data2));
                return;
            }
        }
        panic!("{}", "not expected to reach here");
    }

}
