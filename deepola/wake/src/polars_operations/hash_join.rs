// use polars::series::Series;
use polars::prelude::*;

use crate::data::*;
use crate::graph::ExecutionNode;
use crate::processor::StreamProcessor;

#[derive(Default)]
pub struct HashJoinBuilder {
    left_on: Vec<String>,
    right_on: Vec<String>,
}

impl HashJoinBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn left_on(&mut self, left_on: Vec<String>) -> &mut Self {
        self.left_on = left_on;
        self
    }

    pub fn right_on(&mut self, right_on: Vec<String>) -> &mut Self {
        self.right_on = right_on;
        self
    }

    pub fn build(&self) -> ExecutionNode<DataFrame> {
        let hash_join_node = HashJoinNode::new(&self.left_on, &self.right_on);
        ExecutionNode::<DataFrame>::new(Box::new(hash_join_node), 2)
    }
}

/// A custom SetProcessor<Series> type for reading csv files.
struct HashJoinNode {
    left_on: Vec<String>,
    right_on: Vec<String>,
    right_df: DataFrame,
}

/// A factory method for creating the custom SetProcessor<Series> type for
/// reading csv files
impl HashJoinNode {
    pub fn new(left_on: &[String], right_on: &[String]) -> Self {
        HashJoinNode {
            left_on: left_on.to_owned(),
            right_on: right_on.to_owned(),
            right_df: DataFrame::default(),
        }
    }

    // Read partitions from right stream and append to the existing right dataframe.
    pub fn pre_process(&mut self, right_df: &DataFrame) {
        self.right_df.vstack_mut(right_df).unwrap();
    }

    // Compute Hash Join given left and right df.
    pub fn process(&self, left_df: &DataFrame) -> DataFrame {
        left_df
            .join(
                &self.right_df,
                self.left_on.clone(),
                self.right_on.clone(),
                JoinType::Inner,
                None,
            )
            .unwrap()
    }
}

impl StreamProcessor<DataFrame> for HashJoinNode {
    fn pre_process(&mut self, input_stream: crate::channel::MultiChannelReader<DataFrame>) {
        loop {
            let channel_seq = 1;
            let message = input_stream.read(channel_seq);
            match message.payload() {
                Payload::EOF => {
                    break;
                }
                Payload::Signal(_) => break,
                Payload::Some(dblock) => {
                    self.pre_process(dblock.data());
                }
            }
        }
    }

    fn process_stream(
        &self,
        input_stream: crate::channel::MultiChannelReader<DataFrame>,
        output_stream: crate::channel::MultiChannelBroadcaster<DataFrame>,
    ) {
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            match message.payload() {
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Signal(_) => break,
                Payload::Some(dblock) => {
                    let df = self.process(dblock.data());
                    let message = DataMessage::from(DataBlock::from(df));
                    output_stream.write(message);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::DataMessage;
    use crate::graph::{ExecutionService, NodeReader};
    use crate::polars_operations::CSVReaderBuilder;

    #[test]
    fn test_hash_join_node() {
        // Create a CSV Reader Node with lineitem Schema.
        let csvreader = CSVReaderBuilder::new()
            .delimiter(',')
            .has_headers(true)
            .build();

        // The CSV files that we want to be read by this node => data for DataBlock.
        let input_files = df!(
            "col" => &[
                "resources/tpc-h/data/lineitem-100.csv",
                "resources/tpc-h/data/lineitem-100.csv",
            ]
        )
        .unwrap();
        csvreader.write_to_self(0, DataMessage::from(input_files.clone()));
        csvreader.write_to_self(0, DataMessage::eof());

        let hash_join_node = HashJoinBuilder::new()
            .left_on(vec![
                "l_orderkey".to_string(),
                "l_partkey".to_string(),
                "l_suppkey".to_string(),
            ])
            .right_on(vec![
                "l_orderkey".to_string(),
                "l_partkey".to_string(),
                "l_suppkey".to_string(),
            ])
            .build();
        hash_join_node.subscribe_to_node(&csvreader, 0);
        hash_join_node.subscribe_to_node(&csvreader, 1);
        let reader_node = NodeReader::new(&hash_join_node);

        let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
        service.add(csvreader);
        service.add(hash_join_node);
        service.run();

        let mut total_len = 0;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let data = dblock.data();
            let message_len = data.height();
            total_len += message_len;
        }
        // Right table = 200 (Concatenation of the two tables)
        // Result = (100, ) JOIN (200, ) + (100, ) JOIN (200, )
        assert_eq!(total_len, 400);
    }
}
