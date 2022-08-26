use crate::channel::ChannelWriter;

pub trait Subscribable<T: Send> {
    fn add(&self, channel_writer: ChannelWriter<T>);
}

pub const NODE_ID_ALPHABET: [char; 16] = [
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f',
];

pub const NODE_ID_LEN: usize = 5;
