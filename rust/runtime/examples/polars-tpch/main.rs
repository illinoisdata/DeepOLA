#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate polars;
use crate::utils::TableInput;

use std::collections::HashMap;
use std::time::Instant;
use std::env;

mod utils;
mod q1;
mod q5;

fn main() {
    env_logger::Builder::from_default_env()
    .format_timestamp_micros()
    .init();
    let tpch_tables = vec![
        "lineitem", "orders", "customer", "part", "partsupp", "region", "nation", "supplier"
    ];
    let mut table_input = HashMap::new();
    for tpch_table in tpch_tables {
        table_input.insert(tpch_table.to_string(),
            TableInput {
                batch_size: 100_000,
                input_files: vec![
                    format!("src/resources/tpc-h/scale=1/partition=1/{}.tbl",tpch_table)
                ],
                scale: 1,
            }
        );
    }

    let query = env::args().skip(1).collect::<Vec<String>>();
    println!("Running Query: {}", query[0]);
    let start_time = Instant::now();
    if query.len() != 0 {
        match query[0].as_str() {
            "q1" => { q1::query(table_input) },
            "q5" => { q5::query(table_input) },
            // "q3" => { query_service = q3::query(table_input) },
            // "q5" => { query_service = q5::query(table_input) },
            // "q6" => { query_service = q6::query(table_input) },
            // "q10" => { query_service = q10::query(table_input) },
            // "q12" => { query_service = q12::query(table_input) },
            // "q14" => { query_service = q14::query(table_input) },
            // "q19" => { query_service = q19::query(table_input) },
            _ => panic!("Invalid Query Parameter")
        }
    } else {
        panic!("Query not specified. Run like: cargo run --release --example tpch -- q1")
    }
    let end_time = Instant::now();
    println!("Query Took: {:.2?}", end_time - start_time);
}
