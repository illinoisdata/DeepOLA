use std::{cmp, collections::HashSet};

use crate::data::{ArrayRow, DataBlock, MetaCell, Schema};

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

pub struct SortedArraysJoiner {
    left_join_index: Vec<usize>,
    right_join_index: Vec<usize>,
}

impl SortedArraysJoiner {
    pub fn new(left_join_index: Vec<usize>, right_join_index: Vec<usize>) -> Self {
        SortedArraysJoiner { left_join_index, right_join_index }
    }

    fn construct_meta_of_join(&self, left_schema: &Schema, right_schema: &Schema) -> Schema {
        let mut joined_cols = left_schema.columns.clone();

        // insert the columns not appearing in join keys
        for (rc, ri) in right_schema.columns.iter().enumerate() {
            if self.right_join_index.contains(&rc) {
                continue;
            }
            joined_cols.push(ri.clone());
        }

        Schema::from(joined_cols)
    }

    pub fn inner_join(
        &self,
        left_block: &DataBlock<ArrayRow>,
        right_block: &DataBlock<ArrayRow>,
    ) -> DataBlock<ArrayRow> {
        let mut joined_rows = Vec::<ArrayRow>::new();
        let new_schema = self.construct_meta_of_join(left_block.schema(), right_block.schema());
        let new_metadata = MetaCell::from(new_schema).into_meta_map();

        // extract key indexes
        let left_total_count = left_block.data().len();
        let right_total_count = right_block.data().len();

        // we maintain these iterators to stream-merge two input datasets
        let mut current_left_idx: usize = 0;
        let mut current_right_idx_lb: usize = 0;
        let mut current_right_idx: usize = 0;

        let joiner: SingleArrayJoiner =
            SingleArrayJoiner::new(&self.left_join_index, &self.right_join_index);

        loop {
            if current_left_idx >= left_total_count {
                break;
            }

            // If the right index is at the end of the source, we can treat this same as the case
            // where left value is smaller.
            if current_right_idx >= right_total_count {
                current_left_idx += 1;
                current_right_idx = current_right_idx_lb;
                continue;
            }

            // compare the key values at the current left and right indexes.
            let left_row = &left_block[current_left_idx];
            let right_row = &right_block[current_right_idx];
            match joiner.comp_values_on_key(left_row, right_row) {
                -1 => {
                    // left value is smaller; proceed left index; rollback right index.
                    current_left_idx += 1;
                    current_right_idx = current_right_idx_lb;
                }
                1 => {
                    // right value is smaller; proceed right group index.
                    current_right_idx_lb = current_right_idx + 1;
                    current_right_idx = current_right_idx_lb;
                }
                0 => {
                    // beginning of joining, or keep joining; only right current index proceeds.
                    joined_rows.push(joiner.new_joined_row(left_row, right_row));
                    current_right_idx += 1;
                }
                _ => {
                    // not expected to happen
                }
            }
        }

        DataBlock::new(joined_rows, new_metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::data::{ArrayRow, Column, DataBlock, DataType, MetaCell, DataCell};

    use super::{SingleArrayJoiner, SortedArraysJoiner};

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
        MetaCell::from(vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
            Column::from_field("col4".into(), DataType::Text),
        ])
        .into_meta_map()
    }

    /// Tests inner join using two datasets in one-to-one relationship.
    #[test]
    fn inner_join_one_to_one() {
        let left_block = DataBlock::new(
            vec![
                ArrayRow::from(["left1".into(), "1000".into()]),
                ArrayRow::from(["left2".into(), "1001".into()]),
            ],
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into(), "right1".into()]),
                ArrayRow::from(["1001".into(), "right2".into()]),
            ],
            right_meta(),
        );

        let joiner = SortedArraysJoiner {
            left_join_index: vec![1],
            right_join_index: vec![0],
        };
        let joined = joiner.inner_join(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(
                vec![
                    ArrayRow::from(["left1".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left2".into(), "1001".into(), "right2".into()]),
                ],
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
            ],
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into(), "right1".into()]),
                ArrayRow::from(["1001".into(), "right2".into()]),
            ],
            right_meta(),
        );

        let joiner = SortedArraysJoiner {
            left_join_index: vec![1],
            right_join_index: vec![0],
        };
        let joined = joiner.inner_join(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(
                vec![
                    ArrayRow::from(["left1".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left2".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left3".into(), "1001".into(), "right2".into()]),
                    ArrayRow::from(["left4".into(), "1001".into(), "right2".into()]),
                ],
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
            ],
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into(), "right1".into()]),
                ArrayRow::from(["1000".into(), "right2".into()]),
                ArrayRow::from(["1001".into(), "right3".into()]),
                ArrayRow::from(["1001".into(), "right4".into()]),
            ],
            right_meta(),
        );

        let joiner = SortedArraysJoiner {
            left_join_index: vec![1],
            right_join_index: vec![0],
        };
        let joined = joiner.inner_join(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(
                vec![
                    ArrayRow::from(["left1".into(), "1000".into(), "right1".into()]),
                    ArrayRow::from(["left1".into(), "1000".into(), "right2".into()]),
                    ArrayRow::from(["left2".into(), "1001".into(), "right3".into()]),
                    ArrayRow::from(["left2".into(), "1001".into(), "right4".into()]),
                ],
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
            ],
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into(), "right1".into()]),
                ArrayRow::from(["1000".into(), "right2".into()]),
                ArrayRow::from(["1001".into(), "right3".into()]),
                ArrayRow::from(["1001".into(), "right4".into()]),
            ],
            right_meta(),
        );

        let joiner = SortedArraysJoiner {
            left_join_index: vec![1],
            right_join_index: vec![0],
        };
        let joined = joiner.inner_join(&left_block, &right_block);
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
                ],
                join_meta()
            )
        );
    }

    /// Joining two empty datasets produces an empty set.
    #[test]
    fn inner_join_empty() {
        let left_block = DataBlock::new(vec![], MetaCell::from(vec![]).into_meta_map());
        let right_block = DataBlock::new(vec![], MetaCell::from(vec![]).into_meta_map());

        let joiner = SortedArraysJoiner {
            left_join_index: vec![1],
            right_join_index: vec![0],
        };
        let joined = joiner.inner_join(&left_block, &right_block);
        assert_eq!(
            joined,
            DataBlock::new(vec![], MetaCell::from(vec![]).into_meta_map())
        );
    }

    fn create_meta(col_count: usize) -> HashMap<String, MetaCell> {
        let mut cols = vec![];
        for i in 0..col_count {
            cols.push(Column::from_field(format!("col{}", i), DataType::Text));
        }
        MetaCell::from(cols).into_meta_map()
    }

    fn setup_left_block(row_count: usize, col_count: usize) -> DataBlock<ArrayRow> {
        let mut rows = vec![];
        for r in 0..row_count {
            let mut cols = vec![];
            for i in 0..(col_count-1) {
                cols.push(DataCell::from(format!("col{}", i)));
            }
            cols.push(DataCell::Integer(r.try_into().unwrap()));
            rows.push(ArrayRow::from(cols));
        }
        DataBlock::new(rows, create_meta(col_count))
    }

    fn setup_right_block(row_count: usize, col_count: usize) -> DataBlock<ArrayRow>{
        let mut rows = vec![];
        for r in 0..row_count {
            let mut cols = vec![];
            cols.push(DataCell::Integer(r.try_into().unwrap()));
            for i in 0..(col_count-1) {
                cols.push(DataCell::from(format!("col{}", i)));
            }
            rows.push(ArrayRow::from(cols));
        }
        DataBlock::new(rows, create_meta(col_count))
    }

    #[test]
    fn bench_inner_join() {
        let row_count = 1000;
        let col_count = 10;
        let left_block = setup_left_block(row_count, col_count);
        let right_block = setup_right_block(row_count, col_count);
        let joiner = SortedArraysJoiner::new(vec![col_count-1],vec![0]);

        let joined = joiner.inner_join(&left_block, &right_block);
        println!("{:?}", joined);
    }
}
