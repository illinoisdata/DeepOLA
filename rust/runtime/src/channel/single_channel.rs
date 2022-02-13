use getset::Getters;
use nanoid::nanoid;
use std::sync::mpsc;

use crate::data::DataMessage;

const CHANNEL_SIZE: usize = 1000000;

pub struct Channel;

impl Channel {
    pub fn create<T: Send>() -> (ChannelWriter<T>, ChannelReader<T>) {
        let (channel_tx, channel_rx) = mpsc::sync_channel::<DataMessage<T>>(CHANNEL_SIZE);
        let channel_id = nanoid!(CHANNEL_ID_LEN, &CHANNEL_ID_ALPHABET);
        let writer = ChannelWriter {
            channel_id: channel_id.clone(),
            channel_tx,
        };
        let reader = ChannelReader {
            channel_id: channel_id.clone(),
            channel_rx,
        };
        (writer, reader)
    }
}

const CHANNEL_ID_ALPHABET: [char; 16] = [
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f',
];

const CHANNEL_ID_LEN: usize = 5;

#[derive(Debug, Getters)]
pub struct ChannelWriter<T: Send> {
    #[getset(get = "pub")]
    channel_id: String,

    channel_tx: mpsc::SyncSender<DataMessage<T>>,
}

impl<T: Send> ChannelWriter<T> {
    pub fn write(&self, message: DataMessage<T>) {
        match self.channel_tx.send(message) {
            Ok(_) => (),
            Err(e) => panic!("{}", e.to_string()),
        }
    }
}

impl<T: Send> Clone for ChannelWriter<T> {
    fn clone(&self) -> Self {
        Self {
            channel_id: self.channel_id.clone(),
            channel_tx: self.channel_tx.clone(),
        }
    }
}

/// Reads from a shared channel. There can be only one reader for a channel, which
/// is an important difference than `ChannelWriter` since there can be multiple writers
/// to the same channel.
#[derive(Debug, Getters)]
pub struct ChannelReader<T: Send> {
    #[getset(get = "pub")]
    channel_id: String,

    channel_rx: mpsc::Receiver<DataMessage<T>>,
}

const EMPTY_CHANNEL_MSG: &str = "receiving on an empty channel";

impl<T: Send> ChannelReader<T> {
    pub fn try_read(&self) -> Option<DataMessage<T>> {
        match self.channel_rx.try_recv() {
            Ok(v) => Some(v),
            Err(e) => {
                if e.to_string().eq(&EMPTY_CHANNEL_MSG.to_string()) {
                    None
                } else {
                    panic!("{}", e.to_string())
                }
            }
        }
    }

    pub fn read(&self) -> DataMessage<T> {
        match self.channel_rx.recv() {
            Ok(m) => m,
            Err(e) => {
                panic!("{}", e.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::thread;

    use super::*;

    #[test]
    fn can_move_into_threads() {
        let (reader, writer) = Channel::create::<String>();
        thread::spawn(move || {
            // having drop prevents warning
            drop(reader);
        });
        thread::spawn(move || {
            drop(writer);
        });
    }
}
