use core::time;
use std::thread;

use crate::{
    channel::{MultiChannelBroadcaster, MultiChannelReader},
    data::*,
};

use super::stream_processor::*;
use super::set_processor::*;

pub struct RightCompleteProcessor<T: Send> {
    set_processor: Box<dyn SetMultiProcessor<T>>,
}

impl<T: Send> RightCompleteProcessor<T> {
    fn sleep_micro_secs(&self, micro_secs: u64) {
        let sleep_micros = time::Duration::from_micros(micro_secs);
        thread::sleep(sleep_micros);
    }
}

impl<T: Send> StreamProcessor<T> for RightCompleteProcessor<T> {
    fn process(
        &self,
        input_stream: MultiChannelReader<T>,
        output_stream: MultiChannelBroadcaster<T>,
    ) {
        let left_channel_seq = 0;
        let right_channel_seq = 1;
        let mut processed_right_channel = false;
        loop {
            // Call pre_process on right_channel_seq
            // Keep calling that till it reads an EOF
            let message = input_stream.read(right_channel_seq);
            match message.payload() {
                Payload::Some(dblock) => {
                    // This datablock can update the state.
                    self.set_processor.pre_process(&dblock);
                }
                Payload::EOF => {
                    processed_right_channel = true;
                    break;
                }
                Payload::Signal(_) => break,
            }
            self.sleep_micro_secs(PROCESSOR_SLEEP_MICRO_SECONDS);
        }
        if processed_right_channel {
            // Stream the left channel
            loop {
                let message = input_stream.read(left_channel_seq);
                match message.payload() {
                    Payload::Some(dblock) => {
                        // This datablock can update the state.
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
                self.sleep_micro_secs(PROCESSOR_SLEEP_MICRO_SECONDS);
            }
        }
    }
}

impl<T: Send> From<Box<dyn SetMultiProcessor<T>>> for RightCompleteProcessor<T> {
    fn from(set_processor: Box<dyn SetMultiProcessor<T>>) -> Self {
        RightCompleteProcessor { set_processor }
    }
}