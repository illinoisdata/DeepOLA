use super::base::MergerOp;
use crate::data::{
    DATABLOCK_CARDINALITY,
    DataBlock,
    DataMessage,
    MetaCell,
    Payload,
};
use crate::utils::log_event;
use crate::{
    channel::{MultiChannelBroadcaster, MultiChannelReader},
    processor::StreamProcessor,
};
use getset::{Getters, Setters};
use polars::prelude::DataFrame;
use std::{borrow::Borrow, cell::RefCell, collections::HashMap, sync::Arc};

#[derive(Debug, Clone, PartialEq)]
pub enum MapperDfMergerMode {
    BothOnline,
    LeftOnline,
    RightOnline,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MapperDfEOFBehavior {
    AnyEOF,
    LeftEOF,
    RightEOF,
}

/// Runs the provided mapper on the dataframes obtained from
/// the two streams.
#[derive(Getters, Setters, Clone)]
pub struct MapperDfMerger {
    mapper: Arc<Box<dyn Fn(&DataFrame, &DataFrame) -> DataFrame>>,

    left_dblock: RefCell<Option<DataBlock<DataFrame>>>,

    right_dblock: RefCell<Option<DataBlock<DataFrame>>>,

    needs_left: RefCell<bool>,

    needs_right: RefCell<bool>,

    left_progress: RefCell<f64>,

    right_progress: RefCell<f64>,

    #[set = "pub"]
    #[get = "pub"]
    mode: MapperDfMergerMode,

    #[set = "pub"]
    #[get = "pub"]
    eof_behavior: MapperDfEOFBehavior,
}

unsafe impl Send for MapperDfMerger {}

impl MapperDfMerger {
    fn new() -> Self {
        MapperDfMerger {
            mapper: Arc::new(Box::new(|left_df, _right_df| left_df.clone())),
            left_dblock: RefCell::new(None),
            right_dblock: RefCell::new(None),
            needs_left: RefCell::new(true),
            needs_right: RefCell::new(true),
            left_progress: RefCell::new(0.0),
            right_progress: RefCell::new(0.0),
            mode: MapperDfMergerMode::BothOnline,
            eof_behavior: MapperDfEOFBehavior::AnyEOF,
        }
    }

    pub fn set_mapper(&mut self, mapper: Box<dyn Fn(&DataFrame, &DataFrame) -> DataFrame>) {
        self.mapper = Arc::new(mapper)
    }

    fn current_metadata(&self) -> HashMap<String, MetaCell> {
        // Returns metadata from slow side.
        // TODO: Better?
        if *self.left_progress.borrow() <= *self.right_progress.borrow() {
            self.left_dblock.borrow().as_ref().unwrap().metadata().clone()
        } else {
            self.right_dblock.borrow().as_ref().unwrap().metadata().clone()
        }
    }
}

impl MergerOp<DataFrame> for MapperDfMerger {
    fn new() -> Self {
        MapperDfMerger::new()
    }

    fn merge(&self) -> DataFrame {
        let left = self.left_dblock.borrow();
        let right = self.right_dblock.borrow();
        let left_df: &DataFrame = left.as_ref().unwrap().data();
        let right_df: &DataFrame = right.as_ref().unwrap().data();
        if self.mode == MapperDfMergerMode::BothOnline {
            *self.needs_left.borrow_mut() = true;
            *self.needs_right.borrow_mut() = true;
        } else if self.mode == MapperDfMergerMode::LeftOnline {
            *self.needs_left.borrow_mut() = true;
        } else if self.mode == MapperDfMergerMode::RightOnline {
            *self.needs_right.borrow_mut() = true;
        }
        (self.mapper)(left_df, right_df)
    }

    fn supply_left(&self, data_block: DataBlock<DataFrame>) {
        if let Some(progress) = data_block.metadata().get(DATABLOCK_CARDINALITY) {
            *self.left_progress.borrow_mut() = f64::from(progress);
        } else {
            *self.left_progress.borrow_mut() = 1.0;
        }
        *self.left_dblock.borrow_mut() = Some(data_block);
        *self.needs_left.borrow_mut() = false;
    }

    fn supply_right(&self, data_block: DataBlock<DataFrame>) {
        if let Some(progress) = data_block.metadata().get(DATABLOCK_CARDINALITY) {
            *self.right_progress.borrow_mut() = f64::from(progress);
        } else {
            *self.right_progress.borrow_mut() = 1.0;
        }
        *self.right_dblock.borrow_mut() = Some(data_block);
        *self.needs_right.borrow_mut() = false;
    }

    fn needs_left(&self) -> bool {
        *self.needs_left.borrow()
    }

    fn needs_right(&self) -> bool {
        *self.needs_right.borrow()
    }
}

/// [StreamProcessor] implementation for two channel inputs.
/// Both [SortedDfMerger] and [MapperDfMerger] have the same implementation for [StreamProcessor].
/// To be able to reuse the same code, ideally we should define the implementation for a
/// trait that is implemented by both [MapperDfMerger] and [SortedDfMerger]. But, doing that,
/// the implementation conflicts with the [StreamProcessor] implementation for [MessageProcessor].
/// Hence, duplicating the code currently.
impl StreamProcessor<DataFrame> for MapperDfMerger {
    fn process_stream(
        &self,
        input_stream: MultiChannelReader<DataFrame>,
        output_stream: MultiChannelBroadcaster<DataFrame>,
    ) {
        let channel_left = 0;
        let channel_right = 1;
        let mut eof_left = false;
        let mut eof_right = false;
        loop {
            // if both sides don't need any more inputs, we can merge and produce an output.
            if (eof_left || !self.needs_left().borrow()) && (eof_right || !self.needs_right().borrow()) {
                // merge will set needs_left or needs_right to true; thus, in the next iteration,
                // this condition won't be satisfied.
                log_event("process-message", "start");
                let output_df = self.merge();
                let output_metadata = self.current_metadata();
                let output_dblock = DataBlock::new(output_df, output_metadata);
                let output_message = DataMessage::from(output_dblock);
                output_stream.write(output_message);
                log_event("process-message", "end");
                continue;
            }

            if !eof_left && self.needs_left() {
                let message = input_stream.read(channel_left);
                log_event("process-message", "start");
                match message.payload() {
                    Payload::EOF => {
                        if self.eof_behavior == MapperDfEOFBehavior::AnyEOF || self.eof_behavior == MapperDfEOFBehavior::LeftEOF {
                            output_stream.write(message);
                            log_event("process-message", "end");
                            break;
                        } else {
                            eof_left = true;
                            log_event("process-message", "end");
                            continue;
                        }
                    }
                    Payload::Signal(_) => {
                        log_event("process-message", "end");
                        break;
                    }
                    Payload::Some(data_block) => {
                        self.supply_left(data_block);
                        log_event("process-message", "end");
                        continue;
                    }
                }
            }

            if !eof_right && self.needs_right() {
                let message = input_stream.read(channel_right);
                log_event("process-message", "start");
                match message.payload() {
                    Payload::EOF => {
                        if self.eof_behavior == MapperDfEOFBehavior::AnyEOF || self.eof_behavior == MapperDfEOFBehavior::RightEOF {
                            output_stream.write(message);
                            log_event("process-message", "end");
                            break;
                        } else {
                            eof_right = true;
                            log_event("process-message", "end");
                            continue;
                        }
                    }
                    Payload::Signal(_) => {
                        log_event("process-message", "end");
                        break;
                    }
                    Payload::Some(data_block) => {
                        self.supply_right(data_block);
                        log_event("process-message", "end");
                        continue;
                    }
                }
            }

            // not supposed to hit here
            panic!("Must not reach here.")
        }
    }
}

#[cfg(test)]
mod tests {

    use polars::prelude::*;

    use crate::{data::DataMessage, graph::NodeReader, polars_operations::merger::MergerNode};

    use super::*;

    #[test]
    fn merge_with_identity_mapper() {
        let left_df = df!("left_col" => vec!["hello"], "col" => vec![1]).unwrap();
        let right_df = df!("right_col" => vec!["world"], "col" => vec![1]).unwrap();

        let mut merger = MapperDfMerger::new();
        merger.set_mapper(Box::new(|left_df, _right_df| left_df.clone()));
        let merger_node = MergerNode::<DataFrame, MapperDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df.clone()));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let mut result_count = 0;
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            assert_eq!(output, &left_df);
        }
        assert_eq!(result_count, 1);
    }

    #[test]
    fn merge_with_predicate_mapper() {
        let left_df =
            df!("name" => vec!["first","second","third"], "value" => vec![5,10,15]).unwrap();
        let right_df_1 = df!("value_limit" => vec![7]).unwrap();
        let right_df_2 = df!("value_limit" => vec![12]).unwrap();

        let mut merger = MapperDfMerger::new();
        merger.set_mapper(Box::new(|left_df, right_df| {
            let value_limit = right_df.column("value_limit").unwrap();
            let mask = left_df.column("value").unwrap().gt_eq(value_limit).unwrap();
            left_df.filter(&mask).unwrap()
        }));
        let merger_node = MergerNode::<DataFrame, MapperDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df.clone()));
        merger_node.write_to_self(0, DataMessage::from(left_df));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df_1));
        merger_node.write_to_self(1, DataMessage::from(right_df_2));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let expected_df = vec![
            df!(
                "name" => vec!["second", "third"],
                "value" => vec![10,15]
            )
            .unwrap(),
            df!(
                "name" => vec!["third"],
                "value" => vec![15]
            )
            .unwrap(),
        ];
        let mut result_count = 0;
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            let output = message.datablock().data();
            assert_eq!(output, &expected_df[result_count]);
            result_count += 1;
        }
        assert_eq!(result_count, 2);
    }
}
