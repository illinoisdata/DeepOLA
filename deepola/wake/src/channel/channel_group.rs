use std::rc::Rc;

use crate::data::DataMessage;

use super::single_channel::*;

/// A group of different input channels.
#[derive(Debug)]
pub struct MultiChannelReader<T: Send> {
    pub readers: Vec<Rc<ChannelReader<T>>>,
}

impl<T: Send> Default for MultiChannelReader<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send> MultiChannelReader<T> {
    pub fn new() -> Self {
        Self { readers: vec![] }
    }

    /// Add a new reader. Individual channel readers can be created using
    /// the `Channel` struct.
    pub fn push(&mut self, reader: Rc<ChannelReader<T>>) {
        self.readers.push(reader)
    }

    /// Obtain the seq_no-th reader.
    pub fn reader(&self, seq_no: usize) -> Rc<ChannelReader<T>> {
        (&self.readers[seq_no]).clone()
    }

    /// Read a message from the seq_no-th reader.
    pub fn read(&self, seq_no: usize) -> DataMessage<T> {
        let reader = self.reader(seq_no);
        let message = reader.read();
        log::debug!(
            "Read from (channel: {}). {:?}.",
            reader.channel_id(),
            message
        );
        message
    }
}

impl<T: Send> Clone for MultiChannelReader<T> {
    fn clone(&self) -> Self {
        Self {
            readers: self.readers.clone(),
        }
    }
}

/// A list of channels to broadcast to.
#[derive(Debug)]
pub struct MultiChannelBroadcaster<T: Send> {
    writers: Vec<ChannelWriter<T>>,
}

impl<T: Send> Default for MultiChannelBroadcaster<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send> MultiChannelBroadcaster<T> {
    pub fn new() -> Self {
        Self { writers: vec![] }
    }

    /// Add a new writer.
    pub fn push(&mut self, writer: ChannelWriter<T>) {
        self.writers.push(writer)
    }

    /// Obtain the seq_no-th writer.
    pub fn writer(&self, seq_no: usize) -> ChannelWriter<T> {
        (&self.writers[seq_no]).clone()
    }

    /// Broadcast a message to all writers.
    pub fn write(&self, message: DataMessage<T>) {
        for w in self.iter() {
            log::debug!("Writes to (channel: {}). {:?}.", w.channel_id(), message);
            w.write(message.clone())
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &ChannelWriter<T>> + '_ {
        self.writers.iter()
    }

    pub fn len(&self) -> usize {
        self.writers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Send> Clone for MultiChannelBroadcaster<T> {
    fn clone(&self) -> Self {
        Self {
            writers: self.writers.clone(),
        }
    }
}
