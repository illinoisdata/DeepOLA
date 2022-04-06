use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};
use std::cell::{RefCell};
use rustc_hash::FxHashMap;
use crate::operations::JoinType;
pub struct HashJoinNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// perform Hash Join operation.
impl HashJoinNode {
    pub fn node(left_join_index: Vec<usize>, right_join_index: Vec<usize>, join_type: JoinType) -> ExecutionNode<ArrayRow> {
        let data_processor = HashJoinProcessor::new_boxed(left_join_index, right_join_index, join_type);
        let num_input = 2;
        ExecutionNode::<ArrayRow>::from_right_complete_processor(data_processor,num_input)
    }
}

/// A struct to represent the HashJoinProcessor.
pub struct HashJoinProcessor {
    left_join_index: Vec<usize>,
    right_join_index: Vec<usize>,
    join_type: JoinType,
    hash_table: RefCell<FxHashMap<(u64,usize), Vec<ArrayRow>>>,
    hash_keys: RefCell<FxHashMap<u64, Vec<ArrayRow>>>,
    right_schema: RefCell<Option<Schema>>
}

impl HashJoinProcessor {
    pub fn new(left_join_index: Vec<usize>, right_join_index: Vec<usize>, join_type: JoinType) -> HashJoinProcessor {
        let hash_table = RefCell::new(FxHashMap::default());
        let right_schema = RefCell::new(None);
        let hash_keys = RefCell::new(FxHashMap::default());
        HashJoinProcessor {
            left_join_index,
            right_join_index,
            join_type,
            hash_table,
            hash_keys,
            right_schema,
        }
    }

    pub fn new_boxed(left_join_index: Vec<usize>, right_join_index: Vec<usize>, join_type: JoinType) -> Box<dyn SetMultiProcessor<ArrayRow>> {
        Box::new(Self::new(left_join_index, right_join_index, join_type))
    }

    pub fn _build_output_schema(right_join_index: Vec<usize>, right_schema: Schema, left_schema: Schema) -> Schema {
        let mut joined_cols = left_schema.columns;
        for (rc, ri) in right_schema.columns.iter().enumerate() {
            if right_join_index.contains(&rc) {
                continue;
            }
            joined_cols.push(ri.clone());
        }
        Schema::from(joined_cols)
    }
}

impl SetMultiProcessor<ArrayRow> for HashJoinProcessor {
    /// Updates the input node's hash_table based on records in the right data block.
    fn pre_process(&self, input_set: &DataBlock<ArrayRow>) {
        let mut right_schema = self.right_schema.borrow_mut();
        *right_schema = Some(
                input_set.metadata().get(SCHEMA_META_NAME).unwrap().to_schema().clone()
        );
        let mut hash_table = self.hash_table.borrow_mut();
        let mut hash_keys = self.hash_keys.borrow_mut();

        for record in input_set.data().iter() {
            let key = self.right_join_index
                .iter()
                .map(|a| record[*a].clone())
                .collect::<Vec<DataCell>>();
            let key_hash = DataCell::vector_hash(key.clone());
            let key_row = ArrayRow::from(key.clone());

            // Put array_row main remaining indexes and not the matching keyes.
            let non_key_cols = record.values
                .iter()
                .enumerate()
                .filter(|(i,_)| !self.right_join_index.contains(i))
                .map(|(_,x)| x.clone())
                .collect::<Vec<DataCell>>();
            let val_row = ArrayRow::from(non_key_cols);

            // Check if the key_hash exists in the hash_table or not
            if !hash_keys.contains_key(&key_hash) {
                // hash_keys doesn't contain this hash.
                hash_keys.insert(key_hash, vec![key_row]);
                hash_table.insert((key_hash,0), vec![val_row]);
            } else {
                // Match with the keys in hash_keys.
                let mut found_key = false;
                let mut key_index = 0;
                for (i,existing_key_row) in hash_keys.get(&key_hash).unwrap().iter().enumerate() {
                    if &key_row == existing_key_row {
                        found_key = true;
                        key_index = i;
                        break;
                    }
                }
                if !found_key {
                    hash_table.insert((key_hash,hash_keys.get(&key_hash).unwrap().len()), vec![val_row]);
                    hash_keys.get_mut(&key_hash).unwrap().push(key_row);
                } else {
                    hash_table.get_mut(&(key_hash,key_index)).unwrap().push(val_row);
                }
            }
        }
    }

    fn process<'a>(
        &'a self,
        input_set: &'a DataBlock<ArrayRow>,
    ) -> Generator<'a, (), DataBlock<ArrayRow>> {
        Gn::new_scoped(move |mut s| {
            // HashMap to first group rows and collect by group by keys
            let input_schema = input_set.metadata().get(SCHEMA_META_NAME).unwrap().to_schema();
            let metadata = MetaCell::Schema(Self::_build_output_schema(
                self.right_join_index.clone(),
                self.right_schema.borrow().clone().unwrap(),
                input_schema.clone()
            )).into_meta_map();
            let mut output_records = vec![];
            let hash_table = self.hash_table.borrow();
            let hash_keys = self.hash_keys.borrow();

            for record in input_set.data().iter() {
                // Compute hash based on left_join_index.
                let key = self.left_join_index
                    .iter()
                    .map(|a| record[*a].clone())
                    .collect::<Vec<DataCell>>();
                let key_hash = DataCell::vector_hash(key.clone());
                let key_row = ArrayRow::from(key);

                match self.join_type {
                    JoinType::Inner => {
                        // Create output record only for matching records in both tables.
                        if hash_keys.contains_key(&key_hash) {
                            // Iterate over hash_keys to find index which matches the key_row.__rust_force_expr!
                            let mut key_index = 0;
                            let mut key_found = false;
                            for (i,existing_key_row) in hash_keys.get(&key_hash).unwrap().iter().enumerate() {
                                if &key_row == existing_key_row {
                                    key_index = i;
                                    key_found = true;
                                    break;
                                }
                            }
                            if key_found {
                                for right_record in hash_table.get(&(key_hash,key_index)).unwrap().iter() {
                                    let output_array_row = record.values.iter().chain(right_record.values.iter()).collect::<Vec<&DataCell>>();
                                    // Clone happens when creating the array row here.
                                    output_records.push(ArrayRow::from(output_array_row));
                                }
                            }
                        }
                    },
                }
            }
            let message = DataBlock::new(output_records, metadata);
            s.yield_(message);
            done!();
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        MetaCell::from(vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
            Column::from_field("col4".into(), DataType::Text),
        ])
        .into_meta_map()
    }

    fn left_meta_collision() -> HashMap<String, MetaCell> {
        MetaCell::from(vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
            Column::from_field("col3".into(), DataType::Text),
        ])
        .into_meta_map()
    }

    fn right_meta_collision() -> HashMap<String, MetaCell> {
        MetaCell::from(vec![
            Column::from_field("col4".into(), DataType::Text),
            Column::from_field("col5".into(), DataType::Text),
            Column::from_field("col6".into(), DataType::Text),
        ])
        .into_meta_map()
    }

    fn join_meta_collision() -> HashMap<String, MetaCell> {
        MetaCell::from(vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
            Column::from_field("col3".into(), DataType::Text),
            Column::from_field("col6".into(), DataType::Text)
        ])
        .into_meta_map()
    }

    #[test]
    fn test_hashjoin() {
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
                ArrayRow::from(["1001".into(), "right1".into()]),
                ArrayRow::from(["1001".into(), "right2".into()]),
            ]
            .into(),
            right_meta(),
        );

        // Write the datablocks to left and right channels
        let hashjoin = HashJoinNode::node(vec![1], vec![0], JoinType::Inner);

        // Add block to left channel
        hashjoin.write_to_self(0, DataMessage::from(left_block));
        hashjoin.write_to_self(0, DataMessage::eof());

        // Add block to right channel
        hashjoin.write_to_self(1, DataMessage::from(right_block));
        hashjoin.write_to_self(1, DataMessage::eof());

        let reader_node = NodeReader::new(&hashjoin);
        hashjoin.run();

        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let data = dblock.data();
            let message_len = data.len();
            assert_eq!(message_len, 4);
            assert_eq!(dblock.metadata().clone(), join_meta());
        }
    }

    /// This function tests collision in the hashing function by hashing
    /// keys that include two strings such that their value after
    /// concatentation is same. Since, the vector hashing of DataCell
    /// involves hashing the key one after the other column, a matched
    /// concatenated string representation leads to the same hash.
    /// Hence, "llr" => "ll","r" and "l","lr" hash to the same u64 hash under
    /// the given hashing scheme.
    #[test]
    fn test_hashjoin_collision() {
        let left_block = DataBlock::new(
            vec![
                ArrayRow::from(["left1".into(), "ll".into(), "r".into()]),
                ArrayRow::from(["left2".into(), "l".into(), "lr".into()]),
            ]
            .into(),
            left_meta_collision(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["ll".into(), "r".into(), "right1".into()]),
                ArrayRow::from(["ll".into(), "r".into(), "right2".into()]),
                ArrayRow::from(["l".into(), "lr".into(), "right3".into()]),
                ArrayRow::from(["l".into(), "lr".into(), "right4".into()]),
            ]
            .into(),
            right_meta_collision(),
        );

        // Write the datablocks to left and right channels
        let hashjoin = HashJoinNode::node(vec![1,2], vec![0,1], JoinType::Inner);

        // Add block to left channel
        hashjoin.write_to_self(0, DataMessage::from(left_block));
        hashjoin.write_to_self(0, DataMessage::eof());

        // Add block to right channel
        hashjoin.write_to_self(1, DataMessage::from(right_block));
        hashjoin.write_to_self(1, DataMessage::eof());

        let reader_node = NodeReader::new(&hashjoin);
        hashjoin.run();

        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let data = dblock.data();
            let message_len = data.len();
            assert_eq!(message_len, 4);
            assert_eq!(dblock.metadata().clone(), join_meta_collision());
        }
    }
}