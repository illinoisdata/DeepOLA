use generator::Generator;
use std::collections::HashMap;

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

    // Default implementation duplicates the schema.
    fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
        input_schema.clone()
    }

    // Get input schema.
    fn _get_input_schema(&self, input_metadata: &HashMap<String,MetaCell>) -> Schema {
        input_metadata.get(SCHEMA_META_NAME).unwrap().to_schema().clone()
    }

    // Default implementation copies the data for the standard fields.
    fn _build_output_metadata(&self, input_metadata: &HashMap<String,MetaCell>) -> HashMap<String,MetaCell> {
        let input_schema = input_metadata.get(SCHEMA_META_NAME).unwrap().to_schema();
        let output_schema = self._build_output_schema(input_schema);
        let output_metadata = HashMap::from([
            (SCHEMA_META_NAME.into(), MetaCell::from(output_schema)),
            (DATABLOCK_TYPE.into(), input_metadata.get(DATABLOCK_TYPE).unwrap().clone()),
            (DATABLOCK_CARDINALITY.into(), input_metadata.get(DATABLOCK_CARDINALITY).unwrap().clone())
        ]);
        output_metadata
    }
}

/// SetMultiProcessor is an interface for ExecutionNode
/// where you have to perform node pre-computation on specific input streams
/// before streaming from another channel.
/// An example is RightCompleteProcessor, which first streams the right channel and call pre_process
/// on them till it receives EOF. Then, it streams the left channel and calls process on each message block.
pub trait SetMultiProcessor<T: Send>: Send {
    fn pre_process(&self, dblock: &DataBlock<T>);
    fn process<'a>(&'a self, dblock: &'a DataBlock<T>) -> Generator<'a, (), DataBlock<T>>;

    // Default implementation duplicates the schema.
    fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
        input_schema.clone()
    }

    // Get input schema.
    fn _get_input_schema(&self, input_metadata: &HashMap<String,MetaCell>) -> Schema {
        input_metadata.get(SCHEMA_META_NAME).unwrap().to_schema().clone()
    }

    // Default implementation copies the data for the standard fields.
    fn _build_output_metadata(&self, input_metadata: &HashMap<String,MetaCell>) -> HashMap<String,MetaCell> {
        let input_schema = input_metadata.get(SCHEMA_META_NAME).unwrap().to_schema();
        let output_schema = self._build_output_schema(input_schema);
        let output_metadata = HashMap::from([
            (SCHEMA_META_NAME.into(), MetaCell::from(output_schema)),
            (DATABLOCK_TYPE.into(), input_metadata.get(DATABLOCK_TYPE).unwrap().clone()),
            (DATABLOCK_CARDINALITY.into(), input_metadata.get(DATABLOCK_CARDINALITY).unwrap().clone())
        ]);
        output_metadata
    }
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
            let channel_id = input_stream.reader(channel_seq).channel_id().clone();
            let message = input_stream.read(channel_seq);
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
