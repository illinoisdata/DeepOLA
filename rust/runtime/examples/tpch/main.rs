use crate::utils::TableInput;

extern crate runtime;
use runtime::data::*;
use runtime::graph::*;

use std::collections::HashMap;
use std::time::Instant;
use std::env;

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::Config;

mod utils;
mod q1;
mod q3;
mod q6;
mod q10;
mod q12;
mod q14;
mod q19;

fn main() {
    env_logger::init();
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
                ]
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
            "q3" => { query_service = q3::query(table_input,&mut output_reader); },
            "q6" => { query_service = q6::query(table_input,&mut output_reader); },
            "q10" => { query_service = q10::query(table_input,&mut output_reader); },
            "q12" => { query_service = q12::query(table_input,&mut output_reader); },
            "q14" => { query_service = q14::query(table_input,&mut output_reader); },
            "q19" => { query_service = q19::query(table_input,&mut output_reader); },
            _ => panic!("Invalid Query Parameter")
        }
    } else {
        panic!("Query not specified. Run like: cargo run --release --example tpch -- q1")
    }

    println!("Running Query: {}", query[0]);
    // Display the Query Result and Time Taken to run the query.
    let start_time = Instant::now();
    query_service.run();
    let mut result = DataBlock::from(vec![]);
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
    log::info!("{}", result);
    println!("Query Took: {:.2?}", end_time - start_time);
}
