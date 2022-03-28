use criterion::{criterion_group, criterion_main, Criterion};
use runtime::operations::WhereNode;
use runtime::data::{ArrayRow,DataCell,DataMessage};
use runtime::benchmarks::join_utils::setup_left_block;

pub fn bench_where_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("where_filter");
    let row_count = 1_000_000;
    let col_count = 10;
    let block = setup_left_block(row_count, col_count);

    fn predicate(record: &ArrayRow) -> bool {
        (record.values[9] <= DataCell::from(1_000)) ||
        (record.values[9] >= DataCell::from(100_000))
    }
    let where_node = WhereNode::node(predicate);
    group.sample_size(10);
    group.bench_function("where_filter", |b| {
        b.iter(|| {
            where_node.write_to_self(0, DataMessage::from(block.clone()));
            where_node.write_to_self(0, DataMessage::eof());
            where_node.run();
        })
    });
    group.finish();
}

criterion_group!(benches, bench_where_filter);
criterion_main!(benches);
