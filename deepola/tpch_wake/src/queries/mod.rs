mod q1;
mod q10;
mod q11;
mod q12;
mod q13;
mod q14;
mod q15;
mod q16;
mod q17;
mod q18;
mod q19;
mod q2;
mod q20;
mod q21;
mod q22;
mod q3;
mod q4;
mod q5;
mod q6;
mod q7;
mod q8;
mod q9;

use polars::prelude::DataFrame;
use wake::graph::{ExecutionService, NodeReader};

use crate::{utils, QueryResult};

pub fn run_query_with_args(args: Vec<String>) -> Vec<QueryResult> {
    if args.is_empty() {
        panic!(
            "Query not specified. Run like: cargo run --release --example tpch_polars -- query q1"
        )
    }
    let query_no = args[0].as_str();
    let scale = if args.len() <= 1 {
        1
    } else {
        args[1].parse::<usize>().unwrap()
    };
    let data_directory = if args.len() <= 2 {
        "resources/tpc-h/data/scale=1/partition=10"
    } else {
        args[2].as_str()
    };
    let mut output_reader = NodeReader::empty();
    let mut query_service = get_query_service(query_no, scale, data_directory, &mut output_reader);
    log::info!("Running Query: {}", query_no);
    utils::run_query_with_query_service(query_no, &mut query_service, &mut output_reader)
}

pub fn get_query_service(
    query_no: &str,
    scale: usize,
    data_directory: &str,
    output_reader: &mut NodeReader<DataFrame>,
) -> ExecutionService<DataFrame> {
    let table_input = utils::load_tables(data_directory, scale);
    let query_service = match query_no {
        "q1" => q1::query(table_input, output_reader),
        "q10" => q10::query(table_input, output_reader),
        "q11" => q11::query(table_input, output_reader),
        "q12" => q12::query(table_input, output_reader),
        "q13" => q13::query(table_input, output_reader),
        "q14" => q14::query(table_input, output_reader),
        "q15" => q15::query(table_input, output_reader),
        "q16" => q16::query(table_input, output_reader),
        "q17" => q17::query(table_input, output_reader),
        "q18" => q18::query(table_input, output_reader),
        "q19" => q19::query(table_input, output_reader),
        "q2" => q2::query(table_input, output_reader),
        "q20" => q20::query(table_input, output_reader),
        "q21" => q21::query(table_input, output_reader),
        "q22" => q22::query(table_input, output_reader),
        "q3" => q3::query(table_input, output_reader),
        "q4" => q4::query(table_input, output_reader),
        "q5" => q5::query(table_input, output_reader),
        "q6" => q6::query(table_input, output_reader),
        "q7" => q7::query(table_input, output_reader),
        "q8" => q8::query(table_input, output_reader),
        "q9" => q9::query(table_input, output_reader),
        _ => panic!("Invalid Query Parameter"),
    };
    query_service
}
