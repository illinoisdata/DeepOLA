use super::base::MergerOp;
use crate::data::{DataMessage, Payload};
use crate::{
    channel::{MultiChannelBroadcaster, MultiChannelReader},
    processor::StreamProcessor,
};
use getset::{Getters, Setters};
use polars::{prelude::DataFrame, series::Series};
use std::borrow::Borrow;
use std::cell::RefCell;

/// Inner-joins two dataframes, both of which sorted on their join keys. Assumes that the next data
/// frame (each for left and right) does not contain the same key values appearing in the current
/// data frame.
#[derive(Getters, Setters, Clone)]
pub struct SortedDfMerger {
    // left join key
    #[set = "pub"]
    #[get = "pub"]
    left_on: Vec<String>,

    // right join key
    #[set = "pub"]
    #[get = "pub"]
    right_on: Vec<String>,

    // will be used v2
    left_df: RefCell<Option<DataFrame>>,

    // will be used v2
    right_df: RefCell<Option<DataFrame>>,

    needs_left: RefCell<bool>,

    needs_right: RefCell<bool>,
}

impl SortedDfMerger {
    fn new() -> Self {
        SortedDfMerger {
            left_on: Vec::new(),
            right_on: Vec::new(),
            left_df: RefCell::new(None),
            right_df: RefCell::new(None),
            needs_left: RefCell::new(true),
            needs_right: RefCell::new(true),
        }
    }

    /// Polars library has a bug that if dataframes to join are empty, it panics.
    fn inner_join_possibly_empty(&self, df_left: &DataFrame, df_right: &DataFrame) -> DataFrame {
        if df_left.height() == 0 || df_right.height() == 0 {
            // We will add empty series to this vector
            let mut new_columns = Vec::<Series>::new();
            let zero_length = 0;
            // push all the columns from the left
            for left_col in df_left.get_columns() {
                let empty_col = left_col.sample_n(zero_length, false, false, None).unwrap();
                new_columns.push(empty_col);
            }
            // push all the columns from the right, except for the ones composing join cols
            for right_col in df_right.get_columns() {
                if self.right_on.contains(&String::from(right_col.name())) {
                    continue;
                }
                let empty_col = right_col.sample_n(zero_length, false, false, None).unwrap();
                new_columns.push(empty_col);
            }
            DataFrame::new(new_columns).unwrap()
        } else {
            df_left
                .inner_join(df_right, self.left_on.clone(), self.right_on().clone())
                .unwrap()
        }
    }
}

impl MergerOp<DataFrame> for SortedDfMerger {
    fn new() -> Self {
        SortedDfMerger::new()
    }

    fn merge(&self) -> DataFrame {
        if self.left_on.is_empty() || self.right_on.is_empty() {
            panic!("Empty join columns are not expected.");
        }

        let left = self.left_df.borrow();
        let right = self.right_df.borrow();
        let left_df: &DataFrame = left.as_ref().unwrap();
        let right_df: &DataFrame = right.as_ref().unwrap();

        // checks which side of the dataframes (left or right) ends first.
        // we needs more dataframes from that side.
        let first_row_idx = 0;

        let l_join_max_df = left_df.select(&self.left_on).unwrap().max();
        let l_join_max = l_join_max_df.get(first_row_idx);
        let r_join_max_df = right_df.select(&self.right_on).unwrap().max();
        let r_join_max = r_join_max_df.get(first_row_idx);

        // base cases
        if l_join_max.is_none() {
            *self.needs_left.borrow_mut() = true;
        }
        if r_join_max.is_none() {
            *self.needs_right.borrow_mut() = true;
        }

        if l_join_max.is_some() && r_join_max.is_some() {
            // left df ends first; thus we need another df for the left side
            if l_join_max.as_ref() < r_join_max.as_ref() {
                *self.needs_left.borrow_mut() = true;
                *self.needs_right.borrow_mut() = false;
            }
            // right df ends first; thus we need another df for the right side
            else if l_join_max.as_ref() > r_join_max.as_ref() {
                *self.needs_left.borrow_mut() = false;
                *self.needs_right.borrow_mut() = true;
            }
            // both df end; thus, we need more df for both sides
            else {
                *self.needs_left.borrow_mut() = true;
                *self.needs_right.borrow_mut() = true;
            }
        }

        // actual joined result
        self.inner_join_possibly_empty(left_df, right_df)
    }

    fn supply_left(&self, df_left: &DataFrame) {
        *self.left_df.borrow_mut() = Some(df_left.clone());
        *self.needs_left.borrow_mut() = false;
    }

    fn supply_right(&self, df_right: &DataFrame) {
        *self.right_df.borrow_mut() = Some(df_right.clone());
        *self.needs_right.borrow_mut() = false;
    }

    fn needs_left(&self) -> bool {
        *self.needs_left.borrow()
    }

    fn needs_right(&self) -> bool {
        *self.needs_right.borrow()
    }
}

impl StreamProcessor<DataFrame> for SortedDfMerger {
    fn process_stream(
        &self,
        input_stream: MultiChannelReader<DataFrame>,
        output_stream: MultiChannelBroadcaster<DataFrame>,
    ) {
        let channel_left = 0;
        let channel_right = 1;

        loop {
            // if both sides don't need any more inputs, we can merge and produce an output.
            if !self.needs_left().borrow() && !self.needs_right().borrow() {
                // merge will set needs_left or needs_right to true; thus, in the next iteration,
                // this condition won't be satisfied.
                let df = self.merge();
                output_stream.write(DataMessage::from(df));
                continue;
            }

            if self.needs_left() {
                let message = input_stream.read(channel_left);
                match message.payload() {
                    Payload::EOF => {
                        output_stream.write(message);
                        break;
                    }
                    Payload::Signal(_) => {
                        break;
                    }
                    Payload::Some(data_block) => {
                        let df = data_block.data();
                        self.supply_left(df);
                        continue;
                    }
                }
            }

            if self.needs_right() {
                let message = input_stream.read(channel_right);
                match message.payload() {
                    Payload::EOF => {
                        output_stream.write(message);
                        break;
                    }
                    Payload::Signal(_) => {
                        break;
                    }
                    Payload::Some(data_block) => {
                        let df = data_block.data();
                        self.supply_right(df);
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

    // Checks Polars' function
    // is_empty() returns when no series exists
    #[test]
    fn df_is_empty() {
        let df = df!("col" => Vec::<String>::new()).unwrap();
        assert!(!df.is_empty());
    }

    // Checks Polars' function
    // height() must be used to check the row count
    #[test]
    fn df_zero_row() {
        let df = df!("col" => Vec::<String>::new()).unwrap();
        assert_eq!(df.height(), 0);
    }

    // Checks Polars' join behavior
    #[test]
    fn join_col_appear_middle() {
        let left_df = df!("left_col" => vec!["hello"], "col" => vec![1]).unwrap();
        let right_df = df!("right_col" => vec!["world"], "col" => vec![1]).unwrap();
        let joined_df = left_df
            .inner_join(&right_df, vec!["col"], vec!["col"])
            .unwrap();
        let expected_df = df!(
            "left_col" => vec!["hello"],
            "col" => vec![1],
            "right_col" => vec!["world"])
        .unwrap();
        assert_eq!(joined_df, expected_df);
    }

    #[test]
    fn merge_two_empty() {
        let left_df = df!("col" => Vec::<String>::new()).unwrap();
        let right_df = df!("col" => Vec::<String>::new()).unwrap();

        let mut merger = SortedDfMerger::new();
        merger.set_left_on(vec!["col".into()]);
        merger.set_right_on(vec!["col".into()]);
        let merger_node = MergerNode::<DataFrame, SortedDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let empty_df = df!(
            "col" => Vec::<String>::new()
        )
        .unwrap();

        let mut result_count = 0;
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            assert_eq!(*output, empty_df);
        }
        assert_eq!(result_count, 1);
    }

    #[test]
    fn only_left_empty() {
        let left_df = df!(
            "left_col" => Vec::<String>::new(),
            "col" => Vec::<String>::new()
        )
        .unwrap();
        let right_df = df!(
            "col" => vec!["hello"],
            "right_col" => vec!["world"],
        )
        .unwrap();

        let mut merger = SortedDfMerger::new();
        merger.set_left_on(vec!["col".into()]);
        merger.set_right_on(vec!["col".into()]);
        let merger_node = MergerNode::<DataFrame, SortedDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let empty_df = df!(
            "left_col" => Vec::<String>::new(),
            "col" => Vec::<String>::new(),
            "right_col" => Vec::<String>::new(),
        )
        .unwrap();

        let mut result_count = 0;
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            assert_eq!(*output, empty_df);
        }
        assert_eq!(result_count, 1);
    }

    #[test]
    fn only_right_empty() {
        let left_df = df!(
            "left_col" => vec!["hello"],
            "col" => vec!["world"],
        )
        .unwrap();
        let right_df = df!(
            "col" => Vec::<String>::new(),
            "right_col" => Vec::<String>::new(),
        )
        .unwrap();

        let mut merger = SortedDfMerger::new();
        merger.set_left_on(vec!["col".into()]);
        merger.set_right_on(vec!["col".into()]);
        let merger_node = MergerNode::<DataFrame, SortedDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let empty_df = df!(
            "left_col" => Vec::<String>::new(),
            "col" => Vec::<String>::new(),
            "right_col" => Vec::<String>::new(),
        )
        .unwrap();

        let mut result_count = 0;
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            assert_eq!(*output, empty_df);
        }
        assert_eq!(result_count, 1);
    }

    #[test]
    fn join_on_col() {
        let left_df = df!("left_col" => vec!["hello"], "col" => vec![1]).unwrap();
        let right_df = df!("right_col" => vec!["world"], "col" => vec![1]).unwrap();

        let mut merger = SortedDfMerger::new();
        merger.set_left_on(vec!["col".into()]);
        merger.set_right_on(vec!["col".into()]);
        let merger_node = MergerNode::<DataFrame, SortedDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let expected_joined_df = df!(
            "left_col" => vec!["hello"],
            "col" => vec![1],
            "right_col" => vec!["world"]
        )
        .unwrap();

        let mut result_count = 0;
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            assert_eq!(output, &expected_joined_df);
        }
        assert_eq!(result_count, 1);
    }

    #[test]
    fn right_df_contains_non_matching() {
        let left_df = df!("left_col" => vec!["hello"], "col" => vec![1]).unwrap();
        let right_df = df!("right_col" => vec!["world", "world2"], "col" => vec![1, 2]).unwrap();

        let mut merger = SortedDfMerger::new();
        merger.set_left_on(vec!["col".into()]);
        merger.set_right_on(vec!["col".into()]);
        let merger_node = MergerNode::<DataFrame, SortedDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let expected_joined_df = df!(
            "left_col" => vec!["hello"],
            "col" => vec![1],
            "right_col" => vec!["world"]
        )
        .unwrap();

        let mut result_count = 0;
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            assert_eq!(output, &expected_joined_df);
        }
        assert_eq!(result_count, 1);
    }

    #[test]
    fn left_df_is_taller() {
        let left_df = df!(
                "left_col" => vec!["hello1", "hello2"],
                "col" => vec![1, 2])
        .unwrap();
        let right_df1 = df!(
                "right_col" => vec!["world1"],
                "col" => vec![1])
        .unwrap();
        let right_df2 = df!(
                "right_col" => vec!["world2"],
                "col" => vec![2])
        .unwrap();

        let mut merger = SortedDfMerger::new();
        merger.set_left_on(vec!["col".into()]);
        merger.set_right_on(vec!["col".into()]);
        let merger_node = MergerNode::<DataFrame, SortedDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df1));
        merger_node.write_to_self(1, DataMessage::from(right_df2));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let expected_joined_df = df!(
            "left_col" => vec!["hello1", "hello2"],
            "col" => vec![1, 2],
            "right_col" => vec!["world1", "world2"]
        )
        .unwrap();

        let mut result_count = 0;
        let mut stacked_df = DataFrame::default();
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            stacked_df = stacked_df.vstack(output).unwrap();
        }
        assert_eq!(result_count, 2);
        assert_eq!(stacked_df, expected_joined_df);
    }

    #[test]
    fn right_df_is_taller() {
        let left_df1 = df!(
                "left_col" => vec!["hello1"],
                "col" => vec![1])
        .unwrap();
        let left_df2 = df!(
                "left_col" => vec!["hello2"],
                "col" => vec![2])
        .unwrap();
        let right_df = df!(
                "right_col" => vec!["world1", "world2"],
                "col" => vec![1, 2])
        .unwrap();

        let mut merger = SortedDfMerger::new();
        merger.set_left_on(vec!["col".into()]);
        merger.set_right_on(vec!["col".into()]);
        let merger_node = MergerNode::<DataFrame, SortedDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df1));
        merger_node.write_to_self(0, DataMessage::from(left_df2));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let expected_joined_df = df!(
            "left_col" => vec!["hello1", "hello2"],
            "col" => vec![1, 2],
            "right_col" => vec!["world1", "world2"]
        )
        .unwrap();

        let mut result_count = 0;
        let mut stacked_df = DataFrame::default();
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            stacked_df = stacked_df.vstack(output).unwrap();
        }
        assert_eq!(result_count, 2);
        assert_eq!(stacked_df, expected_joined_df);
    }

    #[test]
    fn left_right_dfs_of_two_chunks() {
        let left_df1 = df!(
                "left_col" => vec!["hello1"],
                "col" => vec![1])
        .unwrap();
        let left_df2 = df!(
                "left_col" => vec!["hello2"],
                "col" => vec![2])
        .unwrap();
        let right_df1 = df!(
                "right_col" => vec!["world1"],
                "col" => vec![1])
        .unwrap();
        let right_df2 = df!(
                "right_col" => vec!["world2"],
                "col" => vec![2])
        .unwrap();

        let mut merger = SortedDfMerger::new();
        merger.set_left_on(vec!["col".into()]);
        merger.set_right_on(vec!["col".into()]);
        let merger_node = MergerNode::<DataFrame, SortedDfMerger>::new()
            .merger(merger)
            .build();
        merger_node.write_to_self(0, DataMessage::from(left_df1));
        merger_node.write_to_self(0, DataMessage::from(left_df2));
        merger_node.write_to_self(0, DataMessage::eof());
        merger_node.write_to_self(1, DataMessage::from(right_df1));
        merger_node.write_to_self(1, DataMessage::from(right_df2));
        merger_node.write_to_self(1, DataMessage::eof());
        let node_reader = NodeReader::new(&merger_node);
        merger_node.run();

        let expected_joined_df = df!(
            "left_col" => vec!["hello1", "hello2"],
            "col" => vec![1, 2],
            "right_col" => vec!["world1", "world2"]
        )
        .unwrap();

        let mut result_count = 0;
        let mut stacked_df = DataFrame::default();
        loop {
            let message = node_reader.read();
            if message.is_eof() {
                break;
            }
            result_count += 1;
            let output = message.datablock().data();
            stacked_df = stacked_df.vstack(output).unwrap();
        }
        assert_eq!(result_count, 2);
        assert_eq!(stacked_df, expected_joined_df);
    }
}
