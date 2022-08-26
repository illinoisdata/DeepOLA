use getset::Getters;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use super::{MetaCell, Schema, SCHEMA_META_NAME};

/// Either actual data (`DataBlock`) or other special signals (e.g., EOF, Signal).
///
/// This is the unit of light-weight clone when exchanging messages via channels. That is,
/// `Arc` provides an efficient cloning mechanism for any arbitrary data stored in
/// `DataBlock`.
#[derive(PartialEq)]
pub enum Payload<T> {
    EOF,
    Some(DataBlock<T>),
    Signal(Signal),
}

impl<T> Payload<T> {
    pub fn new(dblock: DataBlock<T>) -> Self {
        Payload::Some(dblock)
    }

    pub fn data_block(&self) -> &DataBlock<T> {
        match self {
            Self::EOF => panic!(),
            Self::Signal(_) => panic!(),
            Self::Some(dblock) => dblock,
        }
    }
}

impl<T> Clone for Payload<T> {
    fn clone(&self) -> Self {
        match self {
            Self::EOF => Self::EOF,
            Self::Some(records) => Self::Some((*records).clone()),
            Self::Signal(s) => Self::Signal(s.clone()),
        }
    }
}

impl<T> Debug for Payload<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EOF => write!(f, "EOF"),
            Self::Some(dblock) => f.debug_tuple("Data").field(dblock).finish(),
            Self::Signal(s) => f.debug_tuple("Signal").field(s).finish(),
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
/// Introduced to store the index for the primary key. It is efficient to clone DataBlock<T>
/// since the `data` field contains `Arc`.
#[derive(Getters, PartialEq)]
pub struct DataBlock<T> {
    data: Arc<T>,

    #[getset(get = "pub")]
    metadata: HashMap<String, MetaCell>,
}

impl<T> Clone for DataBlock<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

impl<T> DataBlock<T> {
    /// Public constructor.
    pub fn new(data: T, metadata: HashMap<String, MetaCell>) -> Self {
        DataBlock {
            data: Arc::new(data),
            metadata,
        }
    }

    pub fn data(&self) -> &T {
        self.data.as_ref()
    }

    pub fn schema(&self) -> &Schema {
        self.metadata().get(SCHEMA_META_NAME).unwrap().to_schema()
    }
}

impl<T> From<T> for DataBlock<T> {
    fn from(data: T) -> Self {
        Self::new(data, HashMap::new())
    }
}

impl<T> Debug for DataBlock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataBlock").finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::data::schema::Schema;

    /// Simple test of a factory method.
    #[test]
    fn datablock_new() {
        let data: Vec<i64> = vec![19241];
        let metadata = HashMap::from([("key".into(), MetaCell::Text("value".to_string()))]);
        let dblock = DataBlock::new(data.clone(), metadata.clone());
        assert_eq!(dblock.data.as_ref(), &data);
        assert_eq!(dblock.metadata, metadata);
    }

    #[test]
    fn datablock_new_with_schema() {
        let data: Vec<i64> = vec![19241];
        let lineitem_schema = Schema::from_example("lineitem").unwrap();
        let metadata = HashMap::from([
            ("key".into(), MetaCell::Text("value".to_string())),
            ("schema".into(), MetaCell::Schema(lineitem_schema.clone())),
        ]);
        let dblock = DataBlock::new(data.clone(), metadata.clone());
        assert_eq!(dblock.data.as_ref(), &data);
        assert_eq!(dblock.metadata, metadata);
        assert_eq!(
            dblock.metadata.get("schema"),
            Some(&MetaCell::Schema(lineitem_schema))
        );
    }

    /// Even if a payload is cloned, their underlying data objects are the same.
    #[test]
    fn clone_doenst_copy_data() {
        let data: Vec<i64> = vec![19241];
        let metadata = HashMap::from([("key".into(), MetaCell::Text("value".to_string()))]);
        let dblock = DataBlock::new(data.clone(), metadata.clone());
        let payload = Payload::new(dblock);
        let payload_clone = payload.clone();

        if let Payload::Some(dblock) = payload {
            if let Payload::Some(dblock2) = payload_clone {
                let data1 = dblock.data.as_ref();
                let data2 = dblock2.data.as_ref();
                assert!(std::ptr::eq(data1, data2));
                return;
            }
        }
        panic!("{}", "not expected to reach here");
    }
}
