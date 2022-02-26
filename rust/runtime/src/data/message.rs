use getset::Getters;
use std::{ops::Index, sync::Arc, collections::HashMap, fmt::Debug};

use super::payload::*;

/// DataMessage is the unit of exchanging information between execution nodes.
#[derive(Getters)]
pub struct DataMessage<T> {
    #[getset(set = "pub")]
    payload: Payload<T>,
}

impl<T> Index<usize> for DataMessage<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        match &self.payload {
            Payload::Some(dblock) => &dblock.data()[index],
            _ => panic!("index called on non-data."),
        }
    }
}

impl<T> Clone for DataMessage<T> {
    fn clone(&self) -> Self {
        Self { payload: self.payload.clone() }
    }
}

impl<T> Debug for DataMessage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataMessage").field("payload", &self.payload).finish()
    }
}

impl<T> DataMessage<T> {

    pub fn from_single(record: T) -> Self {
        Self::from_set(vec![record])
    }

    pub fn from_set(records: Vec<T>) -> Self {
        Self::from_set_ref(records)
    }

    pub fn from_set_ref(records: Vec<T>) -> Self {
        Self::from_data_block(
            DataBlock::new(records, HashMap::new()))
    }

    pub fn from_data_block(dblock: DataBlock<T>) -> Self {
        Self { payload: Payload::Some(Arc::new(dblock)) }
    }

    pub fn from_data_block_ref(dblock: &Arc<DataBlock<T>>) -> Self {
        Self { payload: Payload::Some(Arc::clone(dblock)) }
    }

    pub fn eof() -> Self {
        Self { payload: Payload::EOF }
    }

    pub fn stop() -> Self {
        Self { payload: Payload::Signal(Signal::STOP) }
    }

    pub fn is_eof(&self) -> bool {
        matches!(self.payload, Payload::EOF)
    }

    pub fn is_present(&self) -> bool {
        !self.is_eof()
    }

    pub fn payload(&self) -> Payload<T> {
        self.payload.clone()
    }

    pub fn len(&self) -> usize {
        match &self.payload {
            Payload::Some(dblock) => dblock.data().len(),
            _ => panic!("len called on non-data."),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item=&T> + '_ {
        match &self.payload {
            Payload::Some(dblock) => dblock.data().iter(),
            _ => panic!("iter called on non-data."),
        }
    }

    pub fn datablock(&self) -> &DataBlock<T> {
        match &self.payload {
            Payload::Some(dblock) => dblock,
            _ => panic!("datablock called on non-data"),
        }
    }
}
