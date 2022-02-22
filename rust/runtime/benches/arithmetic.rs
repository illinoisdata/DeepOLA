use criterion::{criterion_group, criterion_main, Criterion};
use runtime::data::DataCell;

// bench: find the `BENCH_SIZE` first terms of the arithmetic argument
static BENCH_SIZE: i32 = 1000000;

fn base_arithmetic(c: &mut Criterion) {
    c.bench_function("Base Sum", |b| b.iter(||{
        let mut data: Vec<i32> = vec![];
        for i in 0..BENCH_SIZE {
            data.push(i);
        }
        data.iter().sum::<i32>()
    }));
}

fn datacell_arithmetic(c: &mut Criterion) {
    c.bench_function("DataCell Sum", |b| b.iter(||{
        let mut data: Vec<DataCell> = vec![];
        for i in 0..BENCH_SIZE {
            data.push(DataCell::Integer(i))
        }
        DataCell::sum(&data)
    }));
}

criterion_group!(benches, datacell_arithmetic, base_arithmetic);
criterion_main!(benches);
