use getset::Getters;
use std::{ops::Index, sync::Arc};

#[derive(Getters, Debug)]
pub struct DataMessage<T> {
    #[getset(set = "pub")]
    payload: Payload<T>,
}

impl<T> Index<usize> for DataMessage<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        match &self.payload {
            Payload::Some(records) => &records[index],
            _ => panic!("index called on non-data."),
        }
    }
}

impl<T> Clone for DataMessage<T> {
    fn clone(&self) -> Self {
        Self { payload: self.payload.clone() }
    }
}

#[derive(Debug, PartialEq)]
pub enum Payload<T> {
    EOF,
    Some(Arc<Vec<T>>),
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

impl<T> DataMessage<T> {

    pub fn from_single(record: T) -> Self {
        Self::from_set(vec![record])
    }

    pub fn from_set(records: Vec<T>) -> Self {
        Self::from_set_ref(Arc::new(records))
    }

    pub fn from_set_ref(records: Arc<Vec<T>>) -> Self {
        Self { payload: Payload::Some(records) }
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
            Payload::Some(records) => records.len(),
            _ => panic!("len called on non-data."),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item=&T> + '_ {
        match &self.payload {
            Payload::Some(records) => records.iter(),
            _ => panic!("iter called on non-data."),
        }
    }
}
