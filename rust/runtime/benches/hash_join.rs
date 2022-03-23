use criterion::{criterion_group, criterion_main, Criterion};

use runtime::{
    operations::{HashJoinNode, JoinType},
    data::{DataMessage},
    benchmarks::join_utils::{setup_left_block, setup_right_block}
};

pub fn bench_hash_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join");
    let row_count = 1_000_000;
    let col_count = 10;
    let left_block = setup_left_block(row_count, col_count);
    let right_block = setup_right_block(row_count, col_count);

    group.sample_size(10);
    group.bench_function("hash_join", |b| {
        let hashjoin = HashJoinNode::node(
            vec![col_count - 1], vec![0], JoinType::Inner
        );

        // Add block to right channel and process hashing.
        hashjoin.write_to_self(1, DataMessage::from(right_block.clone()));
        hashjoin.write_to_self(1, DataMessage::eof());
        hashjoin.write_to_self(0, DataMessage::eof());
        hashjoin.run();
        println!("FINISHED PRE-PROCESSING PART");

        b.iter(|| {
            // Add block to left channel and process records.
            hashjoin.write_to_self(1, DataMessage::eof());
            hashjoin.write_to_self(0, DataMessage::from(left_block.clone()));
            hashjoin.write_to_self(0, DataMessage::eof());
            hashjoin.run();
        })
    });
    group.finish();
}

criterion_group!(benches, bench_hash_join);
criterion_main!(benches);