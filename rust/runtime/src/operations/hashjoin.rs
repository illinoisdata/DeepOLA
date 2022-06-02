use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};
use std::cell::{RefCell};
use rustc_hash::FxHashMap;
use crate::operations::JoinType;
use std::borrow::Cow;
use std::ops::Deref;
pub struct HashJoinNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// perform Hash Join operation.
impl HashJoinNode {
    pub fn node(left_join_cols: Vec<String>, right_join_cols: Vec<String>, join_type: JoinType) -> ExecutionNode<ArrayRow> {
        let data_processor = HashJoinProcessor::new_boxed(left_join_cols, right_join_cols, join_type);
        let num_input = 2;
        ExecutionNode::<ArrayRow>::from_right_complete_processor(data_processor,num_input)
    }
}

/// A struct to represent the HashJoinProcessor.
pub struct HashJoinProcessor {
    left_join_cols: Vec<String>,
    right_join_cols: Vec<String>,
    join_type: JoinType,
    hash_table: RefCell<FxHashMap<(u64,usize), Vec<ArrayRow>>>,
    hash_keys: RefCell<FxHashMap<u64, Vec<ArrayRow>>>,
    right_schema: RefCell<Option<Schema>>
}

impl HashJoinProcessor {
    pub fn new(left_join_cols: Vec<String>, right_join_cols: Vec<String>, join_type: JoinType) -> HashJoinProcessor {
        let hash_table = RefCell::new(FxHashMap::default());
        let right_schema = RefCell::new(None);
        let hash_keys = RefCell::new(FxHashMap::default());
        HashJoinProcessor {
            left_join_cols,
            right_join_cols,
            join_type,
            hash_table,
            hash_keys,
            right_schema,
        }
    }

    pub fn new_boxed(left_join_cols: Vec<String>, right_join_cols: Vec<String>, join_type: JoinType) -> Box<dyn SetMultiProcessor<ArrayRow>> {
        Box::new(Self::new(left_join_cols, right_join_cols, join_type))
    }
}

impl SetMultiProcessor<ArrayRow> for HashJoinProcessor {
    fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
        let right_join_cols = self.right_join_cols.clone();
        let right_schema = self.right_schema.borrow().clone().unwrap();
        let mut joined_cols = input_schema.columns.clone();
        for (_rc, ri) in right_schema.columns.iter().enumerate() {
            if right_join_cols.contains(&ri.name) {
                continue;
            }
            joined_cols.push(ri.clone());
        }
        log::debug!("HashJoin Output Schema: {}", joined_cols.iter().enumerate().map(|(i,v)| format!("{}: {}, ", i, v.name)).collect::<String>());
        Schema::new(format!("hashjoin({},{})", input_schema.table, right_schema.table), joined_cols)
    }

    /// Updates the input node's hash_table based on records in the right data block.
    fn pre_process(&self, input_set: &DataBlock<ArrayRow>) {
        let mut right_schema = self.right_schema.borrow_mut();
        *right_schema = Some(self._get_input_schema(input_set.metadata()));

        let mut hash_table = self.hash_table.borrow_mut();
        let mut hash_keys = self.hash_keys.borrow_mut();
        let right_join_index = self.right_join_cols.iter().map(|a| right_schema.as_ref().unwrap().index(a)).collect::<Vec<usize>>();

        for record in input_set.data().iter() {
            let key = right_join_index
                .iter()
                .map(|a| record[*a].clone())
                .collect::<Vec<DataCell>>();
            let key_hash = DataCell::vector_hash(key.clone());
            let key_row = ArrayRow::from(key.clone());

            // Put array_row main remaining indexes and not the matching keyes.
            let non_key_cols = record.values
                .iter()
                .enumerate()
                .filter(|(i,_)| !right_join_index.contains(i))
                .map(|(_,x)| x.clone())
                .collect::<Vec<Cow<DataCell>>>();
            let val_row = ArrayRow::from(non_key_cols);

            // Check if the key_hash exists in the hash_table or not
            if let std::collections::hash_map::Entry::Vacant(e) = hash_keys.entry(key_hash) {
                // hash_keys doesn't contain this hash.
                e.insert(vec![key_row]);
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
            let input_schema = self._get_input_schema(input_set.metadata());
            let metadata = self._build_output_metadata(input_set.metadata());
            let mut output_records = vec![];
            let hash_table = self.hash_table.borrow();
            let hash_keys = self.hash_keys.borrow();

            let left_join_index = self.left_join_cols.iter().map(|a| input_schema.index(a)).collect::<Vec<usize>>();

            for record in input_set.data().iter() {
                // Compute hash based on left_join_cols.
                let key = left_join_index
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
                                    let output_array_row = record.values.iter().chain(right_record.values.iter()).map(|x| x.clone()).collect::<Vec<Cow<DataCell>>>();
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
        MetaCell::from(Schema::new("hashjoin(unnamed,unnamed)".into(), vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
            Column::from_field("col4".into(), DataType::Text),
        ]))
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
        MetaCell::from(Schema::new("hashjoin(unnamed,unnamed)".into(), vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
            Column::from_field("col3".into(), DataType::Text),
            Column::from_field("col6".into(), DataType::Text)
        ]))
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
        let hashjoin = HashJoinNode::node(vec!["col2".to_string()], vec!["col3".to_string()], JoinType::Inner);

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
        let hashjoin = HashJoinNode::node(vec!["col2".into(),"col3".into()], vec!["col4".into(),"col5".into()], JoinType::Inner);

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