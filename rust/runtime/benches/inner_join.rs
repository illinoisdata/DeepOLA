use criterion::{black_box, criterion_group, criterion_main, Criterion};

use std::collections::HashMap;

use runtime::{
    data::{ArrayRow, Column, DataBlock, DataCell, DataType, MetaCell},
    operations::{SortedArraysJoiner, JoinType},
};

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
        for i in 0..(col_count - 1) {
            cols.push(DataCell::from(format!("col{}", i)));
        }
        cols.push(DataCell::Integer(r.try_into().unwrap()));
        rows.push(ArrayRow::from(cols));
    }
    DataBlock::new(rows, create_meta(col_count))
}

fn setup_right_block(row_count: usize, col_count: usize) -> DataBlock<ArrayRow> {
    let mut rows = vec![];
    for r in 0..row_count {
        let mut cols = vec![];
        cols.push(DataCell::Integer(r.try_into().unwrap()));
        for i in 0..(col_count - 1) {
            cols.push(DataCell::from(format!("col{}", i)));
        }
        rows.push(ArrayRow::from(cols));
    }
    DataBlock::new(rows, create_meta(col_count))
}

pub fn bench_inner_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("inner_join");
    let row_count = 1000000;
    let col_count = 10;
    let left_block = setup_left_block(row_count, col_count);
    let right_block = setup_right_block(row_count, col_count);
    let joiner = SortedArraysJoiner::new(
        vec![col_count - 1], vec![0], JoinType::Inner);
    group.sample_size(10);
    group.bench_function("inner_join", |b| {
        b.iter(|| joiner.join_blocks(black_box(&left_block), black_box(&right_block)))
    });
    group.finish();
}

criterion_group!(benches, bench_inner_join);
criterion_main!(benches);
