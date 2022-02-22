use criterion::{criterion_group, criterion_main, Criterion};
use runtime::data::DataCell;

// bench: find the `BENCH_SIZE` first terms of the arithmetic argument
static BENCH_SIZE: i32 = 1000000;

fn base_arithmetic_sum_i32(c: &mut Criterion) {
    c.bench_function("Base Sum i32", |b| b.iter(||{
        let mut data: Vec<i32> = vec![];
        for i in 0..BENCH_SIZE {
            data.push(i);
        }
        data.iter().sum::<i32>()
    }));
}

fn base_arithmetic_sum_f64(c: &mut Criterion) {
    c.bench_function("Base Sum f64", |b| b.iter(||{
        let mut data: Vec<f64> = vec![];
        for i in 0..BENCH_SIZE {
            data.push(i.into());
        }
        data.iter().sum::<f64>()
    }));
}

fn datacell_arithmetic_sum_i32(c: &mut Criterion) {
    c.bench_function("DataCell Sum i32", |b| b.iter(||{
        let mut data: Vec<DataCell> = vec![];
        for i in 0..BENCH_SIZE {
            data.push(DataCell::Integer(i))
        }
        DataCell::sum(&data)
    }));
}

fn datacell_arithmetic_sum_f64(c: &mut Criterion) {
    c.bench_function("DataCell Sum f64", |b| b.iter(||{
        let mut data: Vec<DataCell> = vec![];
        for i in 0..BENCH_SIZE {
            data.push(DataCell::Float(i.into()))
        }
        DataCell::sum(&data)
    }));
}


fn base_arithmetic_mul_i32(c: &mut Criterion) {
    c.bench_function("Base Mul i32", |b| b.iter(||{
        let mut result = 1;
        for i in 1..BENCH_SIZE {
            result *= i;
        }
        result
    }));
}

fn datacell_arithmetic_mul_i32(c: &mut Criterion) {
    c.bench_function("DataCell Mul i32", |b| b.iter(||{
        let mut result = DataCell::Integer(1);
        for i in 1..BENCH_SIZE {
            result = result * DataCell::Integer(i);
        }
        result
    }));
}

criterion_group!(arithmetic_benches,
    base_arithmetic_sum_i32,
    datacell_arithmetic_sum_i32,
    base_arithmetic_sum_f64,
    datacell_arithmetic_sum_f64,
    base_arithmetic_mul_i32,
    datacell_arithmetic_mul_i32
);
criterion_main!(arithmetic_benches);
