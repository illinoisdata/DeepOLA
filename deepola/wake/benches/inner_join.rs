// use criterion::{black_box, criterion_group, criterion_main, Criterion};

// use wake::{
//     operations::{SortedArraysJoiner, JoinType},
//     benchmarks::join_utils::{setup_left_block, setup_right_block}
// };

// pub fn bench_inner_join(c: &mut Criterion) {
//     let mut group = c.benchmark_group("inner_join");
//     let row_count = 1000000;
//     let col_count = 10;
//     let left_block = setup_left_block(row_count, col_count);
//     let right_block = setup_right_block(row_count, col_count);
//     let joiner = SortedArraysJoiner::new(
//         vec![col_count - 1], vec![0], JoinType::Inner);
//     group.sample_size(10);
//     group.bench_function("inner_join", |b| {
//         b.iter(|| joiner.join_blocks(black_box(&left_block), black_box(&right_block)))
//     });
//     group.finish();
// }

// criterion_group!(benches, bench_inner_join);
// criterion_main!(benches);
