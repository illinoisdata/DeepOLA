use crate::{
    channel::{MultiChannelBroadcaster, MultiChannelReader},
    data::*,
};

use super::stream_processor::*;
use super::set_processor::*;

pub struct RightCompleteProcessor<T: Send> {
    set_processor: Box<dyn SetMultiProcessor<T>>,
}

impl<T: Send> StreamProcessor<T> for RightCompleteProcessor<T> {
    fn process(
        &self,
        input_stream: MultiChannelReader<T>,
        output_stream: MultiChannelBroadcaster<T>,
    ) {
        let left_channel_seq = 0;
        let right_channel_seq = 1;
        loop {
            // Call pre_process on right_channel_seq
            // Keep calling that till it reads an EOF
            let message = input_stream.read(right_channel_seq);
            match message.payload() {
                Payload::Some(dblock) => {
                    // This datablock can update the node state.
                    self.set_processor.pre_process(&dblock);
                }
                Payload::EOF => {
                    break;
                }
                Payload::Signal(_) => break,
            }
        }
        // Stream the left channel
        loop {
            let message = input_stream.read(left_channel_seq);
            match message.payload() {
                Payload::Some(dblock) => {
                    let generator = self.set_processor.process(&dblock);
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

impl<T: Send> From<Box<dyn SetMultiProcessor<T>>> for RightCompleteProcessor<T> {
    fn from(set_processor: Box<dyn SetMultiProcessor<T>>) -> Self {
        RightCompleteProcessor { set_processor }
    }
}