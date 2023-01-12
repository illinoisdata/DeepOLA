#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate wake;
use env_logger::Target;
use polars::prelude::DataFrame;
use std::env;
use wake::graph::*;

mod q1;
mod q1c;
mod q10;
mod q11;
mod q12;
mod q13;
mod q14;
mod q14c;
mod q15;
mod q16;
mod q17;
mod q18;
mod q19;
mod q2;
mod q20;
mod q21;
mod q22;
mod q23; // WanderJoin Q3
mod q24; // WanderJoin Q10
mod q25; // WanderJoin Q7
mod q26; // ProgressiveDB Q1
mod q27; // ProgressiveDB Q6
mod q3;
mod q4;
mod q5;
mod q5c;
mod q6;
mod q6c;
mod q7;
mod q8;
mod q8c;
mod q9;
mod prelude;
mod utils;

fn main() {
    // Arguments:
    // 0: Whether to run query or test. Required. query/test.
    // 1: Query Number. Required.
    // 2: Scale of the TPC-H Dataset. Optional. Default: 1.
    // 3: Directory containing the dataset. Optional. Default: resources/tpc-h/data/scale=1/partition=1/
    // 4: Directory where we should write the output. Optional. Default: outputs/{query_no}

    env_logger::Builder::from_default_env()
        .format_timestamp_micros()
        .target(Target::Stdout)
        .init();

    let args = env::args().skip(1).collect::<Vec<String>>();
    match args[0].as_str() {
        // "test" => tests::test_tpch_query(args[1].as_str()),
        "query" => run_query(args.into_iter().skip(1).collect::<Vec<String>>()),
        _ => panic!(
            "Invalid Argument to the cargo run command.
        Run: `cargo run --release --example tpch_polars -- query q1` to run query q1.
        Run `cargo run --release --example tpch_polars -- test` to run test for query q1."
        ),
    }
}

fn run_query(args: Vec<String>) {
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
        "resources/tpc-h/data/scale=1/partition=10/tbl"
    } else {
        args[2].as_str()
    };
    let result_dir = if args.len() <= 3 {
        format!("outputs/{}", query_no)
    } else {
        args[3].clone()
    };
    let experiment = if args.len() <= 4 {
        "latency"
    } else {
        args[4].as_str()
    };
    log::warn!("Saving outputs in {:?}", result_dir);
    let mut output_reader = NodeReader::empty();
    let mut query_service = get_query_service(query_no, scale, data_directory, &mut output_reader);
    log::info!("Running Query: {}", query_no);
    utils::run_query(query_no, &mut query_service, &mut output_reader, result_dir, experiment);
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
        "q1c" => q1c::query(table_input, output_reader),
        "q10" => q10::query(table_input, output_reader),
        "q11" => q11::query(table_input, output_reader),
        "q12" => q12::query(table_input, output_reader),
        "q13" => q13::query(table_input, output_reader),
        "q14" => q14::query(table_input, output_reader),
        "q14c" => q14c::query(table_input, output_reader),
        "q15" => q15::query(table_input, output_reader),
        "q16" => q16::query(table_input, output_reader),
        "q17" => q17::query(table_input, output_reader),
        "q18" => q18::query(table_input, output_reader),
        "q19" => q19::query(table_input, output_reader),
        "q2" => q2::query(table_input, output_reader),
        "q20" => q20::query(table_input, output_reader),
        "q21" => q21::query(table_input, output_reader),
        "q22" => q22::query(table_input, output_reader),
        "q23" => q23::query(table_input, output_reader),
        "q24" => q24::query(table_input, output_reader),
        "q25" => q25::query(table_input, output_reader),
        "q26" => q26::query(table_input, output_reader),
        "q27" => q27::query(table_input, output_reader),
        "q3" => q3::query(table_input, output_reader),
        "q4" => q4::query(table_input, output_reader),
        "q5" => q5::query(table_input, output_reader),
        "q5c" => q5c::query(table_input, output_reader),
        "q6" => q6::query(table_input, output_reader),
        "q6c" => q6c::query(table_input, output_reader),
        "q7" => q7::query(table_input, output_reader),
        "q8" => q8::query(table_input, output_reader),
        "q8c" => q8c::query(table_input, output_reader),
        "q9" => q9::query(table_input, output_reader),
        _ => panic!("Invalid Query Parameter"),
    };
    query_service
}
