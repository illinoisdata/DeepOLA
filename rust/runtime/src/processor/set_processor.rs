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

/// SetMultiProcessor is an interface for ExecutionNode
/// where you have to perform node pre-computation on specific input streams
/// before streaming from another channel.
/// An example is RightCompleteProcessor, which first streams the right channel and call pre_process
/// on them till it receives EOF. Then, it streams the left channel and calls process on each message block.
pub trait SetMultiProcessor<T: Send>: Send {
    fn pre_process(&self, dblock: &DataBlock<T>);
    fn process<'a>(&'a self, dblock: &'a DataBlock<T>) -> Generator<'a, (), DataBlock<T>>;
}


/// A wrapper for SetProcessorV1 to support StreamProcessor.
/// Hnadles a single input channel.
pub struct SimpleStreamProcessor<T: Send> {
    set_processor: Box<dyn SetProcessorV1<T>>,
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
                        output_stream.write(DataMessage::<T>::from(dblock));
                    }
                }
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Signal(_) => break,
            }
        }
    }
}

impl<T: Send> From<Box<dyn SetProcessorV1<T>>> for SimpleStreamProcessor<T> {
    fn from(set_processor: Box<dyn SetProcessorV1<T>>) -> Self {
        SimpleStreamProcessor { set_processor }
    }
}

pub const PROCESSOR_SLEEP_MICRO_SECONDS: u64 = 1;
