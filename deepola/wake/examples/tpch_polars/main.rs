#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use crate::utils::TableInput;

extern crate wake;
use glob::glob;
use polars::prelude::DataFrame;
use wake::data::*;
use wake::graph::*;

use std::collections::HashMap;
use std::env;
use std::time::Instant;

mod q1;
mod q3;
mod q14;
mod utils;

fn load_tables(directory: &str, scale: usize) -> HashMap<String, TableInput> {
    log::info!("Specified Input Directory: {}", directory);

    let tpch_tables = vec![
        "lineitem", "orders", "customer", "part", "partsupp", "region", "nation", "supplier",
    ];
    let mut table_input = HashMap::new();
    for tpch_table in tpch_tables {
        let mut input_files = vec![];
        for entry in glob(&format!("{}/{}.tbl*", directory, tpch_table))
            .expect("Failed to read glob pattern")
        {
            match entry {
                Ok(path) => input_files.push(path.to_str().unwrap().to_string()),
                Err(e) => println!("{:?}", e),
            }
        }
        // To sort slices correctly taking into account the partition numbers.
        alphanumeric_sort::sort_str_slice(&mut input_files);
        table_input.insert(
            tpch_table.to_string(),
            TableInput {
                input_files: input_files,
                scale: scale,
            },
        );
    }
    log::info!("Evaluating On Files");
    log::info!("{:?}", table_input);
    table_input
}

fn run_query(
    query_service: &mut ExecutionService<DataFrame>,
    output_reader: &mut NodeReader<DataFrame>,
) {
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
    log::info!("Query Took: {:.2?}", end_time - start_time);
}

fn main() {
    // Arguments:
    // Query Number (Required). Example: q1
    // Scale (Optional). Default: 1
    // Data Directory (Optional). Default: resources/tpc-h/data/scale=1/partition=1/
    env_logger::Builder::from_default_env()
        .format_timestamp_micros()
        .init();

    let mut output_reader = NodeReader::empty();
    let mut query_service;
    let query = env::args().skip(1).collect::<Vec<String>>();
    if query.len() != 0 {
        let query_no = query[0].as_str();
        let scale = if query.len() <= 1 {
            1
        } else {
            *(&query[1].parse::<usize>().unwrap())
        };
        let data_directory = if query.len() <= 2 {
            "resources/tpc-h/data/scale=1/partition=1"
        } else {
            query[2].as_str()
        };
        let table_input = load_tables(data_directory, scale);
        match query_no {
            "q1" => {
                query_service = q1::query(table_input, &mut output_reader);
            }
            "q3" => {
                query_service = q3::query(table_input, &mut output_reader);
            }
            "q14" => {
                query_service = q14::query(table_input, &mut output_reader);
            }
            _ => panic!("Invalid Query Parameter"),
        }
        log::info!("Running Query: {}", query_no);
        run_query(&mut query_service, &mut output_reader);
    } else {
        panic!("Query not specified. Run like: cargo run --release --example tpch_polars -- q1")
    }
}
