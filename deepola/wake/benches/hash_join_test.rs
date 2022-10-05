// use criterion::{criterion_group, criterion_main, Criterion};
// use polars::prelude::*;
// use polars::frame::hash_join::{get_chunked_array, get_created_hash_table};

// pub fn bench_hash_join_with_hash_table(c: &mut Criterion) {
//     let mut group = c.benchmark_group("hash_join");

//     group.sample_size(10);
//     group.bench_function("hash_join_w_ht", |b| {
//         let right_filename = "src/resources/tpc-h/lineitem_1M.tbl";
//         let right_reader = polars::prelude::CsvReader::from_path(right_filename)
//             .unwrap()
//             .has_header(false)
//             .with_delimiter('|' as u8);
//         let right_df = right_reader.finish().unwrap();

//         let chunked_array = get_chunked_array(&right_df, vec!["column_16".to_string()]);
//         let hash_table = get_created_hash_table(&chunked_array);

//         b.iter(|| {
//             for partition in 1..6 {
//                 println!("Running Partition: {}", partition);
//                 let left_filename = format!("src/resources/tpc-h/scale=1/partition=5/lineitem.tbl.{}", partition);
//                 let left_reader = polars::prelude::CsvReader::from_path(left_filename)
//                     .unwrap()
//                     .has_header(false)
//                     .with_delimiter('|' as u8);
//                 let left_df = left_reader.finish().unwrap();
//                 let result_df = left_df.join(&right_df, vec!["column_16"], vec!["column_16"], JoinType::Inner, None, Some(hash_table.clone())).unwrap();
//                 criterion::black_box(result_df);
//             }
//         })
//     });
//     group.finish();
// }

// pub fn bench_hash_join_without_hash_table(c: &mut Criterion) {
//     let mut group = c.benchmark_group("hash_join");

//     group.sample_size(10);
//     group.bench_function("hash_join_wo_ht", |b| {
//         let right_filename = "src/resources/tpc-h/lineitem_1M.tbl";
//         let right_reader = polars::prelude::CsvReader::from_path(right_filename)
//             .unwrap()
//             .has_header(false)
//             .with_delimiter('|' as u8);
//         let right_df = right_reader.finish().unwrap();

//         b.iter(|| {
//             for partition in 1..6 {
//                 println!("Running Partition: {}", partition);
//                 let left_filename = format!("src/resources/tpc-h/scale=1/partition=5/lineitem.tbl.{}", partition);
//                 let left_reader = polars::prelude::CsvReader::from_path(left_filename)
//                     .unwrap()
//                     .has_header(false)
//                     .with_delimiter('|' as u8);
//                 let left_df = left_reader.finish().unwrap();
//                 let result_df = left_df.join(&right_df, vec!["column_16"], vec!["column_16"], JoinType::Inner, None, None).unwrap();
//                 criterion::black_box(result_df);
//             }
//         })
//     });
//     group.finish();
// }

// criterion_group!(benches, bench_hash_join_with_hash_table, bench_hash_join_without_hash_table);
// criterion_main!(benches);
