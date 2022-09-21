use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use tpch_wake::run_query_with_args;

fn benchmark_tpch_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("TPC-H Queries");
    group.sample_size(10);
    for query_no in 1..23 {
        group.bench_function(format!("TPC-H q{}", query_no), |b| {
            b.iter(|| {
                let query = format!("q{}", query_no);
                let run_query_args = vec![
                    query,
                    "1".into(),
                    "resources/tpc-h/data/scale=1/partition=10".into(),
                ];
                let _query_results = run_query_with_args(run_query_args);
            });
        });
    }
}

criterion_group!(tpch_benches, benchmark_tpch_queries);
criterion_main!(tpch_benches);
