use super::channel::ChannelWriter;

pub trait Subscribable<T: Send> {
    fn add(&self, channel_writer: ChannelWriter<T>);
}

#[derive(PartialEq)]
pub enum NodeState {
    /// A thread is not running.
    STOPPED,

    /// A signal is set to stop as early as possible.
    STOPPING,

    /// A tread is running to process input records as quickly as possible.
    RUNNING,
}

pub const NODE_ID_ALPHABET: [char; 16] = [
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f',
];

pub const NODE_ID_LEN: usize = 5;

pub enum ExecStatus {
    Ok(usize),
    EmptyChannel,
    EOF,
    Err(String),
}

pub const NODE_SLEEP_MICRO_SECONDS: u64 = 1;
