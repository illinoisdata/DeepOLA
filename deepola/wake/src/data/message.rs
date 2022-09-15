use getset::Getters;
use std::fmt::Debug;

use super::payload::*;

/// DataMessage is the unit of exchanging information between execution nodes.
#[derive(Getters)]
pub struct DataMessage<T> {
    #[getset(set = "pub")]
    payload: Payload<T>,
}

impl<T> Clone for DataMessage<T> {
    fn clone(&self) -> Self {
        Self {
            payload: self.payload.clone(),
        }
    }
}

impl<T> Debug for DataMessage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataMessage")
            .field("payload", &self.payload)
            .finish()
    }
}

impl<T> From<DataBlock<T>> for DataMessage<T> {
    fn from(dblock: DataBlock<T>) -> Self {
        Self {
            payload: Payload::Some(dblock),
        }
    }
}

impl<T> From<T> for DataMessage<T> {
    fn from(data: T) -> Self {
        Self::from(DataBlock::from(data))
    }
}

impl<T> DataMessage<T> {
    pub fn from_single(record: T) -> Self {
        Self::from(DataBlock::from(record))
    }

    pub fn eof() -> Self {
        Self {
            payload: Payload::EOF,
        }
    }

    pub fn stop() -> Self {
        Self {
            payload: Payload::Signal(Signal::STOP),
        }
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

    pub fn datablock(&self) -> &DataBlock<T> {
        match &self.payload {
            Payload::Some(dblock) => dblock,
            _ => panic!("datablock called on non-data"),
        }
    }
}
