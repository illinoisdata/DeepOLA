use std::{cell::RefCell, cmp, collections::HashSet, mem};

use crate::{
    channel::{MultiChannelBroadcaster, MultiChannelReader},
    data::{ArrayRow, DataBlock, DataMessage, MetaCell, Schema},
    graph::ExecutionNode,
    processor::StreamProcessor,
};

// A builder for creating a join node.
pub struct MergeJoinBuilder {

    // The column index (of the left table) to join on.
    left_on_index: Option<Vec<usize>>,

    // The column index (of the right table) to join on.
    right_on_index: Option<Vec<usize>>,

    // One of `inner`, `left`, `right`, and `full`.
    join_type: JoinType,
}

impl Default for MergeJoinBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeJoinBuilder {
    pub fn new() -> Self {
        MergeJoinBuilder {
            left_on_index: Some(vec![]),
            right_on_index: Some(vec![]),
            join_type: JoinType::Inner,
        }
    }

    pub fn left_on_index(&mut self, names: Vec<usize>) -> &mut Self {
        self.left_on_index = Some(names);
        self
    }

    pub fn right_on_index(&mut self, names: Vec<usize>) -> &mut Self {
        self.right_on_index = Some(names);
        self
    }

    pub fn build(&self) -> ExecutionNode<ArrayRow> {
        let stream_processor = SortedArraysJoiner::new(
            self.left_on_index.clone().unwrap(),
            self.right_on_index.clone().unwrap(),
            self.join_type.clone(),
        );
        let num_input = 2;
        ExecutionNode::<ArrayRow>::new(Box::new(stream_processor), num_input)
    }
}

/// A collection of functions that can be used for stream-join two sorted datasets.
///
/// Requirement:
///
/// 1. Both the left and the right table must be sorted on `left_join_index` and
/// `right_join_index`, respectively.
/// 2. The records with same key may appear on two subsequent right blocks, but NOT on **three**
/// subsequent right blocks. (No requirement exists for left blocks, however. This requirement on
/// the right block is because we need to backtrace if there are multiple records with the same
/// join key.
///
/// How this join algorithm operates:
///
/// 1. The basic concept: For each record of the left table, we find the matching right join records
/// and add them to the result set. Once we process a block of left records, we produce an output
/// block. This process occurs for each invocation of the `join()` method.
/// 2. The `join()` method may not produce an output if we cannot retrieve a complete set of
/// right records (associated with the same key value). In this case, the caller must provide an
/// additional right record block by calling `offer_right_block()`.
/// 3. The caller may terminate the join operation if all the left blocks have been consumed.
pub struct SortedArraysJoiner {
    left_join_index: Vec<usize>,
    right_join_index: Vec<usize>,

    // One of `inner`, `left`, `right`, and `full`.
    join_type: JoinType,

    // These are internal fields used for tracking the current state of join operations.
    join_in_progress: RefCell<JoinInProgress>,
    schema_of_joined: RefCell<Option<Schema>>,
}

/// Internal struct for tracking join progress.
struct JoinInProgress {
    current_left_block: JoinBlock<ArrayRow>,
    current_left_idx: usize,
    current_right_block: JoinBlock<ArrayRow>,
    current_right_idx_lb: usize,
    current_right_idx: usize,
    so_far_joined: Vec<ArrayRow>,
}

#[derive(Clone)]
pub enum JoinType {
    Inner,
}

enum JoinBlock<T> {
    Block(DataBlock<T>), // regular case
    NeverSet,            // initial status
    NoMore,              // after the last block
}

impl<T> From<DataBlock<T>> for JoinBlock<T> {
    fn from(dblock: DataBlock<T>) -> Self {
        Self::Block(dblock)
    }
}

impl<T> From<&DataBlock<T>> for JoinBlock<T> {
    fn from(dblock: &DataBlock<T>) -> Self {
        Self::from(dblock.clone())
    }
}

impl<'a> StreamProcessor<ArrayRow> for SortedArraysJoiner {
    fn process(
        &self,
        input_stream: MultiChannelReader<ArrayRow>,
        output_stream: MultiChannelBroadcaster<ArrayRow>,
    ) {
        let mut response;
        loop {
            response = match &self.join_type {
                JoinType::Inner => self.try_produce_inner_join(),
            };
            if let Some(dblock) = response.data_block {
                output_stream.write(DataMessage::from(dblock));
            }
            match response.status {
                MergeJoinStatus::LeftConsumed | MergeJoinStatus::NoLeftBlock => {
                    let message = input_stream.read(0);
                    if message.is_eof() {
                        break;
                    }
                    let dblock = message.datablock();
                    self.offer_left_block(JoinBlock::from(dblock));
                }
                MergeJoinStatus::RightConsumed => {
                    let message = input_stream.read(1);
                    match message.is_eof() {
                        true => self.offer_right_block(JoinBlock::NoMore),
                        false => {
                            let dblock = message.datablock();
                            self.offer_right_block(JoinBlock::from(dblock));
                        }
                    }
                },
                MergeJoinStatus::NoRightBlock => {
                    let message = input_stream.read(1);
                    let dblock = message.datablock();
                    self.offer_right_block(JoinBlock::from(dblock))
                }
            };
        }
        output_stream.write(DataMessage::eof());
    }
}

impl SortedArraysJoiner {
    pub fn new(
        left_join_index: Vec<usize>,
        right_join_index: Vec<usize>,
        join_type: JoinType,
    ) -> Self {
        SortedArraysJoiner {
            left_join_index,
            right_join_index,
            join_type,
            join_in_progress: RefCell::new(JoinInProgress {
                current_left_block: JoinBlock::NeverSet,
                current_left_idx: 0,
                current_right_block: JoinBlock::NeverSet,
                current_right_idx_lb: 0,
                current_right_idx: 0,
                so_far_joined: vec![],
            }),
            schema_of_joined: RefCell::new(None),
        }
    }

    fn offer_left_block(&self, data_block: JoinBlock<ArrayRow>) {
        let mut join_data = self.join_in_progress.borrow_mut();
        join_data.current_left_block = data_block;
        join_data.current_left_idx = 0;
    }

    fn offer_right_block(&self, data_block: JoinBlock<ArrayRow>) {
        let mut join_data = self.join_in_progress.borrow_mut();
        join_data.current_right_block = data_block;
        join_data.current_right_idx_lb = 0;
        join_data.current_right_idx = 0;
    }

    fn create_status_result(&self, status: MergeJoinStatus) -> JoinResult<ArrayRow> {
        JoinResult::<ArrayRow> {
            data_block: None,
            status,
        }
    }

    // TODO: Add _build_output_metadata() that takes into account the cardinality of left and right block.
    fn create_joined_result(
        joined_rows: &mut Vec<ArrayRow>,
        schema: Option<Schema>,
    ) -> JoinResult<ArrayRow> {
        let mut joined = vec![];
        mem::swap(joined_rows, &mut joined);

        JoinResult::<ArrayRow> {
            data_block: Some(DataBlock::new(
                joined,
                MetaCell::from(schema.unwrap()).into_meta_map(),
            )),
            status: MergeJoinStatus::LeftConsumed,
        }
    }

    /// Either produces an output or inform that the next block is needed.
    fn try_produce_inner_join(&self) -> JoinResult<ArrayRow> {
        let join_data = &mut *self.join_in_progress.borrow_mut();

        let left_block = match &join_data.current_left_block {
            JoinBlock::NeverSet => return self.create_status_result(MergeJoinStatus::NoLeftBlock),
            JoinBlock::NoMore => return self.create_status_result(MergeJoinStatus::LeftConsumed),
            JoinBlock::Block(dblock) => dblock.clone(),
        };
        let right_block = match &join_data.current_right_block {
            JoinBlock::NeverSet => return self.create_status_result(MergeJoinStatus::NoRightBlock),
            JoinBlock::NoMore => {
                return Self::create_joined_result(
                    &mut join_data.so_far_joined,
                    self.schema_of_joined.borrow().clone(),
                );
            }
            JoinBlock::Block(dblock) => dblock.clone()
        };

        // we maintain these iterators to stream-merge two input datasets
        let current_left_idx = &mut join_data.current_left_idx;
        let current_right_idx_lb = &mut join_data.current_right_idx_lb;
        let current_right_idx = &mut join_data.current_right_idx;

        *self.schema_of_joined.borrow_mut() = Self::construct_meta_of_join(
            &self.right_join_index,
            left_block.schema(),
            right_block.schema(),
        );
        let joined_rows = &mut join_data.so_far_joined;

        // extract key indexes
        let left_total_count = left_block.data().len();
        let right_total_count = right_block.data().len();

        let joiner: SingleArrayJoiner =
            SingleArrayJoiner::new(&self.left_join_index, &self.right_join_index);

        loop {
            if *current_left_idx >= left_total_count {
                break;
            }

            // If the right group index is at the end of the source, we request an additional
            // right block.
            if *current_right_idx_lb >= right_total_count {
                return self.create_status_result(MergeJoinStatus::RightConsumed);
            }

            // If the right index is at the end of the source, we can treat this same as the case
            // where left value is smaller.
            if *current_right_idx >= right_total_count {
                *current_left_idx += 1;
                *current_right_idx = *current_right_idx_lb;
                continue;
            }

            // compare the key values at the current left and right indexes.
            let left_row = &left_block[*current_left_idx];
            let right_row = &right_block[*current_right_idx];
            match joiner.comp_values_on_key(left_row, right_row) {
                -1 => {
                    // left value is smaller; proceed left index; rollback right index.
                    *current_left_idx += 1;
                    *current_right_idx = *current_right_idx_lb;
                }
                1 => {
                    // right value is smaller; proceed right group index.
                    *current_right_idx_lb = *current_right_idx + 1;
                    *current_right_idx = *current_right_idx_lb;
                }
                0 => {
                    // beginning of joining, or keep joining; only right current index proceeds.
                    joined_rows.push(joiner.new_joined_row(left_row, right_row));
                    *current_right_idx += 1;
                }
                _ => {
                    // not expected to happen
                }
            }
        }

        Self::create_joined_result(joined_rows, self.schema_of_joined.borrow().clone())
    }

    fn construct_meta_of_join(
        right_join_index: &[usize],
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Option<Schema> {
        let mut joined_cols = left_schema.columns.clone();

        // insert the columns not appearing in join keys
        for (rc, ri) in right_schema.columns.iter().enumerate() {
            if right_join_index.contains(&rc) {
                continue;
            }
            joined_cols.push(ri.clone());
        }
        log::debug!("Merge Join Output Schema: {}", joined_cols.iter().enumerate().map(|(i,v)| format!("{}: {}, ", i, v.name)).collect::<String>());
        Some(Schema::new(format!("mergejoin({},{})",left_schema.table, right_schema.table),joined_cols))
    }

    /// This is a convenience method only used for testing.
    pub fn join_blocks(
        &self,
        left_block: &DataBlock<ArrayRow>,
        right_block: &DataBlock<ArrayRow>,
    ) -> DataBlock<ArrayRow> {
        self.offer_left_block(JoinBlock::from(left_block));
        self.offer_right_block(JoinBlock::from(right_block));

        let mut result: JoinResult<ArrayRow>;
        loop {
            result = match self.join_type {
                JoinType::Inner => self.try_produce_inner_join(),
            };

            match result.status {
                MergeJoinStatus::LeftConsumed => break,
                MergeJoinStatus::RightConsumed => self.offer_right_block(JoinBlock::NoMore),
                _ => break,
            }
        }

        result.data_block.unwrap()
    }
}

struct JoinResult<T> {
    data_block: Option<DataBlock<T>>,
    status: MergeJoinStatus,
}

enum MergeJoinStatus {
    LeftConsumed,    // regular case where we just processed a left block
    RightConsumed,   // regular case where we just processed a right block
    NoLeftBlock,     // initial state
    NoRightBlock,    // initial state
}

struct SingleArrayJoiner {
    left_join_index: Vec<usize>,
    right_join_index: Vec<usize>,
    right_index_set: HashSet<usize>,
}

impl SingleArrayJoiner {
    pub fn new(left_join_index: &[usize], right_join_index: &[usize]) -> Self {
        SingleArrayJoiner {
            left_join_index: left_join_index.to_vec(),
            right_join_index: right_join_index.to_vec(),
            right_index_set: HashSet::from_iter(right_join_index.to_owned().into_iter()),
        }
    }

    /// Compares which row comes first based on their respective key columns.
    ///
    /// Returns -1 if left_row is smaller. Returns 1 if right_row is smaller. Returns
    /// 0 if both rows are the same.
    fn comp_values_on_key(&self, left_row: &ArrayRow, right_row: &ArrayRow) -> i8 {
        let left_join_index = &self.left_join_index;
        let right_join_index = &self.right_join_index;

        let left_len = left_join_index.len();
        let right_len = right_join_index.len();
        let min_len = cmp::min(left_len, right_len);

        for i in 0..min_len {
            let li = left_join_index[i];
            let ri = right_join_index[i];
            if left_row[li] < right_row[ri] {
                return -1;
            } else if left_row[li] > right_row[ri] {
                return 1;
            }
        }

        // At this point all the items up until `min_len` are identical.
        match left_len.cmp(&right_len) {
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
        }
    }

    /// First copy all values from the left; and then copy the values from the right, except for
    /// the ones used for the join.
    ///
    /// Before using this function, the comparison check on join keys must have been performed to
    /// ensure valid results.
    fn new_joined_row(&self, left_row: &ArrayRow, right_row: &ArrayRow) -> ArrayRow {
        let mut joined = left_row.values.clone();
        let right_row_len = right_row.len();
        for i in 0..right_row_len {
            if self.right_index_set.contains(&i) {
                continue;
            }
            joined.push(right_row[i].clone());
        }
        ArrayRow::from(joined)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        data::{ArrayRow, Column, DataBlock, DataType, MetaCell, DataMessage, Schema},
        operations::JoinType, graph::NodeReader,
    };

    use super::{MergeJoinBuilder, SingleArrayJoiner, SortedArraysJoiner};

    #[test]
    fn can_join_on_single_key() {
        let left = ArrayRow::from(["value1".into(), "value2".into()]);
        let right = ArrayRow::from(["value2".into(), "value3".into()]);
        let expected_joined = ArrayRow::from(["value1".into(), "value2".into(), "value3".into()]);
        let joiner = SingleArrayJoiner::new(&vec![1], &vec![0]);
        let actual_joined = joiner.new_joined_row(&left, &right);
        assert_eq!(actual_joined, expected_joined);
    }

    #[test]
    fn can_join_on_two_keys() {
        let left = ArrayRow::from(["value1".into(), "value2".into(), "value3".into()]);
        let right = ArrayRow::from(["value4".into(), "value2".into(), "value3".into()]);
        let expected_joined = ArrayRow::from([
            "value1".into(),
            "value2".into(),
            "value3".into(),
            "value4".into(),
        ]);
        let joiner = SingleArrayJoiner::new(&vec![1, 2], &vec![1, 2]);
        let actual_joined = joiner.new_joined_row(&left, &right);
        assert_eq!(actual_joined, expected_joined);
    }

    fn left_meta() -> HashMap<String, MetaCell> {
        MetaCell::from(vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
        ])
        .into_meta_map()
    }

    fn right_meta() -> HashMap<String, MetaCell> {
        MetaCell::from(vec![
            Column::from_field("col3".into(), DataType::Text),
            Column::from_field("col4".into(), DataType::Text),
        ])
        .into_meta_map()
    }

    fn join_meta() -> HashMap<String, MetaCell> {
        MetaCell::from(Schema::new( "mergejoin(unnamed,unnamed)".into(), vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
            Column::from_field("col4".into(), DataType::Text),
        ]))
        .into_meta_map()
    }

    /// Tests inner join using two datasets in one-to-one relationship.
    #[test]
    fn inner_join_one_to_one() {
        let left_block = DataBlock::new(
            vec![
                ArrayRow::from(["left1".into(), "1000".into()]),
                ArrayRow::from(["left2".into(), "1001".into()]),
            ]
            .into(),
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into(), "right1".into()]),
                ArrayRow::from(["1001".into(), "right2".into()]),
            ]
            .into(),
            right_meta(),
        );

        let joiner = SortedArraysJoiner::new(vec![1], vec![0], JoinType::Inner);
        let joined = joiner.join_blocks(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(
                vec![
                    ArrayRow::from(["left1".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left2".into(), "1001".into(), "right2".into()]),
                ]
                .into(),
                join_meta()
            )
        );
    }

    /// Left table has multiple matching join key.
    #[test]
    fn inner_join_many_to_one() {
        let left_block = DataBlock::new(
            vec![
                ArrayRow::from(["left1".into(), "1000".into()]),
                ArrayRow::from(["left2".into(), "1000".into()]),
                ArrayRow::from(["left3".into(), "1001".into()]),
                ArrayRow::from(["left4".into(), "1001".into()]),
            ]
            .into(),
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into(), "right1".into()]),
                ArrayRow::from(["1001".into(), "right2".into()]),
            ]
            .into(),
            right_meta(),
        );

        let joiner = SortedArraysJoiner::new(vec![1], vec![0], JoinType::Inner);
        let joined = joiner.join_blocks(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(
                vec![
                    ArrayRow::from(["left1".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left2".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left3".into(), "1001".into(), "right2".into()]),
                    ArrayRow::from(["left4".into(), "1001".into(), "right2".into()]),
                ]
                .into(),
                join_meta()
            )
        );
    }

    /// Right table has multiple matching join key.
    #[test]
    fn inner_join_one_to_many() {
        let left_block = DataBlock::new(
            vec![
                ArrayRow::from(["left1".into(), "1000".into()]),
                ArrayRow::from(["left2".into(), "1001".into()]),
            ]
            .into(),
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into(), "right1".into()]),
                ArrayRow::from(["1000".into(), "right2".into()]),
                ArrayRow::from(["1001".into(), "right3".into()]),
                ArrayRow::from(["1001".into(), "right4".into()]),
            ]
            .into(),
            right_meta(),
        );

        let joiner = SortedArraysJoiner::new(vec![1], vec![0], JoinType::Inner);
        let joined = joiner.join_blocks(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(
                vec![
                    ArrayRow::from(["left1".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left1".into(), "1000".into(), "right2".into()]),
                    ArrayRow::from(["left2".into(), "1001".into(), "right3".into()]),
                    ArrayRow::from(["left2".into(), "1001".into(), "right4".into()]),
                ]
                .into(),
                join_meta()
            )
        );
    }

    /// Both left and right tables have multiple matching join key.
    #[test]
    fn inner_join_many_to_many() {
        let left_block = DataBlock::new(
            vec![
                ArrayRow::from(["left1".into(), "1000".into()]),
                ArrayRow::from(["left2".into(), "1000".into()]),
                ArrayRow::from(["left3".into(), "1001".into()]),
                ArrayRow::from(["left4".into(), "1001".into()]),
            ]
            .into(),
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into(), "right1".into()]),
                ArrayRow::from(["1000".into(), "right2".into()]),
                ArrayRow::from(["1001".into(), "right3".into()]),
                ArrayRow::from(["1001".into(), "right4".into()]),
            ]
            .into(),
            right_meta(),
        );

        let joiner = SortedArraysJoiner::new(vec![1], vec![0], JoinType::Inner);
        let joined = joiner.join_blocks(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(
                vec![
                    ArrayRow::from(["left1".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left1".into(), "1000".into(), "right2".into()]),
                    ArrayRow::from(["left2".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left2".into(), "1000".into(), "right2".into()]),
                    ArrayRow::from(["left3".into(), "1001".into(), "right3".into()]),
                    ArrayRow::from(["left3".into(), "1001".into(), "right4".into()]),
                    ArrayRow::from(["left4".into(), "1001".into(), "right3".into()]),
                    ArrayRow::from(["left4".into(), "1001".into(), "right4".into()]),
                ]
                .into(),
                join_meta()
            )
        );
    }

    /// Joining two empty datasets produces an empty set.
    #[test]
    fn inner_join_empty() {
        let left_block = DataBlock::new(vec![].into(), MetaCell::from(vec![]).into_meta_map());
        let right_block = DataBlock::new(vec![].into(), MetaCell::from(vec![]).into_meta_map());

        let joiner = SortedArraysJoiner::new(vec![1], vec![0], JoinType::Inner);
        let joined = joiner.join_blocks(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(vec![].into(), MetaCell::from(Schema::new("mergejoin(unnamed,unnamed)".into(), vec![])).into_meta_map())
        );
    }

    #[test]
    fn left_block_has_extra_key() {
        let left_block = DataBlock::new(
            vec![ArrayRow::from(["left1".into(), "1001".into()])].into(),
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![ArrayRow::from(["1000".into(), "right1".into()])].into(),
            right_meta(),
        );

        let joiner = SortedArraysJoiner::new(vec![1], vec![0], JoinType::Inner);
        let joined = joiner.join_blocks(&left_block, &right_block);
        assert_eq!(joined, DataBlock::new(vec![].into(), join_meta()));
    }

    #[test]
    fn right_block_has_extra_key() {
        let left_block = DataBlock::new(
            vec![ArrayRow::from(["left1".into(), "1000".into()])].into(),
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![ArrayRow::from(["1001".into(), "right1".into()])].into(),
            right_meta(),
        );

        let joiner = SortedArraysJoiner::new(vec![1], vec![0], JoinType::Inner);
        let joined = joiner.join_blocks(&left_block, &right_block);
        assert_eq!(joined, DataBlock::new(vec![].into(), join_meta()));
    }

    // 1000 and 1001 are the matching keys
    #[test]
    fn join_node() {
        let join_node = MergeJoinBuilder::new()
            .left_on_index(vec![1])
            .right_on_index(vec![0])
            .build();
        let left_block1 = DataBlock::new(
            vec![ArrayRow::from(["left1".into(), "1000".into()])].into(),
            left_meta(),
        );
        let left_block2 = DataBlock::new(
            vec![ArrayRow::from(["left1".into(), "1001".into()])].into(),
            left_meta(),
        );
        let right_block1 = DataBlock::new(
            vec![ArrayRow::from(["1000".into(), "right1".into()])].into(),
            right_meta(),
        );
        let right_block2 = DataBlock::new(
            vec![ArrayRow::from(["1001".into(), "right1".into()])].into(),
            right_meta(),
        );

        join_node.write_to_self(0, DataMessage::from(left_block1));
        join_node.write_to_self(0, DataMessage::from(left_block2));
        join_node.write_to_self(1, DataMessage::from(right_block1));
        join_node.write_to_self(1, DataMessage::from(right_block2));
        join_node.write_to_self(0, DataMessage::eof());
        join_node.write_to_self(1, DataMessage::eof());
        let reader_node = NodeReader::new(&join_node);
        join_node.run();

        let mut total_count = 0;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            total_count += message.datablock().len();
        }
        assert_eq!(total_count, 2);
    }

    // 1000 is the only matching key
    #[test]
    fn join_node2() {
        let join_node = MergeJoinBuilder::new()
            .left_on_index(vec![1])
            .right_on_index(vec![0])
            .build();
        let left_block1 = DataBlock::new(
            vec![ArrayRow::from(["left1".into(), "1000".into()])].into(),
            left_meta(),
        );
        let left_block2 = DataBlock::new(
            vec![ArrayRow::from(["left1".into(), "1001".into()])].into(),
            left_meta(),
        );
        let right_block1 = DataBlock::new(
            vec![ArrayRow::from(["1000".into(), "right1".into()])].into(),
            right_meta(),
        );
        let right_block2 = DataBlock::new(
            vec![ArrayRow::from(["1002".into(), "right1".into()])].into(),
            right_meta(),
        );

        join_node.write_to_self(0, DataMessage::from(left_block1));
        join_node.write_to_self(0, DataMessage::from(left_block2));
        join_node.write_to_self(1, DataMessage::from(right_block1));
        join_node.write_to_self(1, DataMessage::from(right_block2));
        join_node.write_to_self(0, DataMessage::eof());
        join_node.write_to_self(1, DataMessage::eof());
        let reader_node = NodeReader::new(&join_node);
        join_node.run();

        let mut total_count = 0;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            total_count += message.datablock().len();
        }
        assert_eq!(total_count, 1);
    }

    // No matching key
    #[test]
    fn join_node3() {
        let join_node = MergeJoinBuilder::new()
            .left_on_index(vec![1])
            .right_on_index(vec![0])
            .build();
        let left_block1 = DataBlock::new(
            vec![ArrayRow::from(["left1".into(), "999".into()])].into(),
            left_meta(),
        );
        let left_block2 = DataBlock::new(
            vec![ArrayRow::from(["left1".into(), "1001".into()])].into(),
            left_meta(),
        );
        let right_block1 = DataBlock::new(
            vec![ArrayRow::from(["1000".into(), "right1".into()])].into(),
            right_meta(),
        );
        let right_block2 = DataBlock::new(
            vec![ArrayRow::from(["1002".into(), "right1".into()])].into(),
            right_meta(),
        );

        join_node.write_to_self(0, DataMessage::from(left_block1));
        join_node.write_to_self(0, DataMessage::from(left_block2));
        join_node.write_to_self(1, DataMessage::from(right_block1));
        join_node.write_to_self(1, DataMessage::from(right_block2));
        join_node.write_to_self(0, DataMessage::eof());
        join_node.write_to_self(1, DataMessage::eof());
        let reader_node = NodeReader::new(&join_node);
        join_node.run();

        let mut total_count = 0;
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            total_count += message.datablock().len();
        }
        assert_eq!(total_count, 0);
    }

}
