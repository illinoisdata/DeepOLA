use crate::utils::TableInput;

extern crate runtime;
use runtime::data::*;
use runtime::graph::NodeReader;

use std::collections::HashMap;
use std::time::Instant;
use std::env;

mod utils;
mod q1;

fn main() {
    let mut table_input = HashMap::new();
    table_input.insert("lineitem".to_string(),
        TableInput {
            batch_size: 100_000,
            input_files: vec![
                "src/resources/tpc-h/scale=1/partition=1/lineitem.tbl".into()
            ]
        }
    );
    // Create an empty NodeReader
    let mut output_reader = NodeReader::empty();

    let mut query_service;
    let query = env::args().skip(1).collect::<Vec<String>>();
    if query.len() != 0 {
        match query[0].as_str() {
            "q1" => {
                query_service = q1::query(table_input,&mut output_reader);
            },
            // "q12" => {
            //     query_service = q12::query(table_input,&mut output_reader);
            // }
            _ => panic!("Invalid Query Parameter")
        }
    } else {
        panic!("Query not specified. Run like: cargo run --release --example tpch -- q1")
    }

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
    println!("Query Result");
    println!("{}", result);
    println!("Query took {:.2?}", end_time - start_time);
}
