#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use crate::utils::TableInput;

extern crate wake;
use polars::prelude::DataFrame;
use wake::data::*;
use wake::graph::*;
use glob::glob;

use std::collections::HashMap;
use std::time::Instant;
use std::env;

mod utils;
mod q1;

fn main() {
    env_logger::Builder::from_default_env()
    .format_timestamp_micros()
    .init();
    let tpch_tables = vec![
        "lineitem", "orders", "customer", "part", "partsupp", "region", "nation", "supplier"
    ];
    let mut table_input = HashMap::new();
    for tpch_table in tpch_tables {
        let mut input_files = vec![];
        for entry in glob(&format!("resources/tpc-h/data/scale=1/partition=1/{}.tbl*",tpch_table)).expect("Failed to read glob pattern") {
            match entry {
                Ok(path) => input_files.push(path.to_str().unwrap().to_string()),
                Err(e) => println!("{:?}", e),
            }
        }
        table_input.insert(tpch_table.to_string(),
            TableInput {
                batch_size: 100_000,
                input_files: input_files,
                scale: 1,
            }
        );
    }

    // Create an empty NodeReader
    let mut output_reader = NodeReader::empty();

    let mut query_service;
    let query = env::args().skip(1).collect::<Vec<String>>();
    if query.len() != 0 {
        match query[0].as_str() {
            "q1" => { query_service = q1::query(table_input,&mut output_reader); },
            _ => panic!("Invalid Query Parameter")
        }
    } else {
        panic!("Query not specified. Run like: cargo run --release --example tpch_polars -- q1")
    }

    log::info!("Running Query: {}", query[0]);
    // Display the Query Result and Time Taken to run the query.
    let start_time = Instant::now();
    query_service.run();
    let mut result = DataBlock::from(DataFrame::default());
    loop {
        let message = output_reader.read();
        if message.is_eof() {
            break;
        }
        let data = message.datablock();
        result = data.clone();
    }
    query_service.join();
    let end_time = Instant::now();
    log::info!("Query Result");
    log::info!("{:?}", result.data());
    println!("Query Took: {:.2?}", end_time - start_time);
}
