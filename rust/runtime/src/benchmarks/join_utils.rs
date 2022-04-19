use std::collections::HashMap;
use crate::data::{ArrayRow, Column, DataBlock, DataCell, DataType, MetaCell};

pub fn create_meta(col_count: usize) -> HashMap<String, MetaCell> {
    let mut cols = vec![];
    for i in 0..col_count {
        cols.push(Column::from_field(format!("col{}", i), DataType::Text));
    }
    MetaCell::from(cols).into_meta_map()
}

pub fn setup_left_block(row_count: usize, col_count: usize) -> DataBlock<ArrayRow> {
    let mut rows = vec![];
    for r in 0..row_count {
        let mut cols = vec![];
        for i in 0..(col_count - 1) {
            cols.push(DataCell::from(format!("col{}", i)));
        }
        cols.push(DataCell::Integer(r.try_into().unwrap()));
        rows.push(ArrayRow::from(cols));
    }
    DataBlock::new(rows, create_meta(col_count))
}

pub fn setup_right_block(row_count: usize, col_count: usize) -> DataBlock<ArrayRow> {
    let mut rows = vec![];
    for r in 0..row_count {
        let mut cols = vec![DataCell::Integer(r.try_into().unwrap())];
        for i in 0..(col_count - 1) {
            cols.push(DataCell::from(format!("col{}", i)));
        }
        rows.push(ArrayRow::from(cols));
    }
    DataBlock::new(rows, create_meta(col_count))
}