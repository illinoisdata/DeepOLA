use core::time;
use std::thread;

use generator::Generator;

use crate::{
    channel::{MultiChannelBroadcaster, MultiChannelReader},
    data::*,
};

use super::stream_processor::*;

/// An old, deprecated interface for ExecutionNode.
///
/// This is the interface for processing a set of input dataset and produces
/// a set of output dataset, in each iteration. ExecutioNode repeats such an
/// iteration indefinitely until it sees EOF.
///
/// Since this is a generic trait, concrete implementations must be provided.
/// See [`SimpleMap`] for an example
pub trait SetProcessorV1<T: Send>: Send {
    fn process_v1<'a>(&'a self, dblock: &'a DataBlock<T>) -> Generator<'a, (), DataBlock<T>>;
}

/// A wrapper for SetProcessorV1 to support StreamProcessor.
/// Hnadles a single input channel.
pub struct SimpleStreamProcessor<T: Send> {
    set_processor: Box<dyn SetProcessorV1<T>>,
}

impl<T: Send> SimpleStreamProcessor<T> {
    fn sleep_micro_secs(&self, micro_secs: u64) {
        let sleep_micros = time::Duration::from_micros(micro_secs);
        thread::sleep(sleep_micros);
    }
}

impl<T: Send> StreamProcessor<T> for SimpleStreamProcessor<T> {
    fn process(
        &self,
        input_stream: MultiChannelReader<T>,
        output_stream: MultiChannelBroadcaster<T>,
    ) {
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            match message.payload() {
                Payload::Some(dblock) => {
                    let generator = self.set_processor.process_v1(&dblock);
                    for dblock in generator {
                        output_stream.write(DataMessage::<T>::from_data_block(dblock));
                    }
                }
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Signal(_) => break,
            }
            self.sleep_micro_secs(PROCESSOR_SLEEP_MICRO_SECONDS);
        }
    }
}

impl<T: Send> From<Box<dyn SetProcessorV1<T>>> for SimpleStreamProcessor<T> {
    fn from(set_processor: Box<dyn SetProcessorV1<T>>) -> Self {
        SimpleStreamProcessor { set_processor }
    }
}

pub const PROCESSOR_SLEEP_MICRO_SECONDS: u64 = 1;
