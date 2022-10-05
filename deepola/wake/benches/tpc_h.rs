// use criterion::{criterion_group, criterion_main, Criterion};
// use wake::examples::tpch_polars::utils;

// pub fn bench_tpch_queries(c: &mut Criterion) {
//     let mut group = c.benchmark_group("tpch_queries");
//     group.sample_size(10);

//     let registered_queries = vec!["q1", "q2", "q3", "q6", "q10", "q14", "q17", "q18", "q19"];
//     for query_no in registered_queries {
//         group.bench_function(format!("tpch_query_{}", query_no), |b| {
//             let query_args = vec![
//                 query_no,
//                 "1",
//                 "resources/tpc-h/data/scale=1/partition=10"
//             ];

//             b.iter(|| {
//                 utils::run_query(query_args);
//             })
//         });
//     }
//     group.finish();
// }

// criterion_group!(benches, bench_tpch_queries);
// criterion_main!(benches);
