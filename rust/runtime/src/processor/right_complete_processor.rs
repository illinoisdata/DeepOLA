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
        let mut channel_id;
        loop {
            // Call pre_process on right_channel_seq
            // Keep calling that till it reads an EOF
            let message = input_stream.read(right_channel_seq);
            channel_id = input_stream.reader(right_channel_seq).channel_id().clone();
            match message.payload() {
                Payload::Some(dblock) => {
                    // This datablock can update the node state.
                    let input_schema_table = match dblock.metadata().get(SCHEMA_META_NAME) {
                        Some(schema) => schema.to_schema().table.clone(),
                        None => "unnamed".to_string()
                    };
                    let input_cardinality = match dblock.metadata().get(DATABLOCK_CARDINALITY) {
                        Some(value) => f64::from(value),
                        None => 0.0,
                    };
                    log::debug!("Channel: {} read Schema Name: {} with Cardinality: {:.2}", channel_id, input_schema_table, input_cardinality);
                    self.set_processor.pre_process(&dblock);
                }
                Payload::EOF => {
                    break;
                }
                Payload::Signal(_) => break,
            }
        }
        log::debug!("Channel: {} Pre-Process Completed", channel_id);
        // Stream the left channel
        loop {
            let message = input_stream.read(left_channel_seq);
            let channel_id = input_stream.reader(left_channel_seq).channel_id().clone();
            match message.payload() {
                Payload::Some(dblock) => {
                    let input_schema_table = match dblock.metadata().get(SCHEMA_META_NAME) {
                        Some(schema) => schema.to_schema().table.clone(),
                        None => "unnamed".to_string()
                    };
                    let input_cardinality = match dblock.metadata().get(DATABLOCK_CARDINALITY) {
                        Some(value) => f64::from(value),
                        None => 0.0,
                    };
                    log::debug!("Channel: {} read Schema Name: {} with Cardinality: {:.2}", channel_id, input_schema_table, input_cardinality);
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