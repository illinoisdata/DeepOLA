#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate wake;
use polars::prelude::DataFrame;
use std::env;
use wake::graph::*;

mod q1;
mod q10;
mod q14;
mod q17;
mod q18;
mod q19;
mod q2;
mod q3;
mod q6;
mod utils;

fn main() {
    // Arguments:
    // 0: Whether to run query or test. Required. query/test.
    // 1: Query Number. Required.
    // 2: Scale of the TPC-H Dataset. Optional. Default: 1.
    // 3: Directory containing the dataset. Optional. Default: resources/tpc-h/data/scale=1/partition=1/

    env_logger::Builder::from_default_env()
        .format_timestamp_micros()
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
    if args.len() == 0 {
        panic!("Query not specified. Run like: cargo run --release --example tpch_polars -- query q1")
    }
    let query_no = args[0].as_str();
    let scale = if args.len() <= 1 {
        1
    } else {
        *(&args[1].parse::<usize>().unwrap())
    };
    let data_directory = if args.len() <= 2 {
        "resources/tpc-h/data/scale=1/partition=1"
    } else {
        args[2].as_str()
    };
    let mut output_reader = NodeReader::empty();
    let mut query_service = get_query_service(query_no, scale, data_directory, &mut output_reader);
    log::info!("Running Query: {}", query_no);
    utils::run_query(&mut query_service, &mut output_reader);
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
        "q14" => q14::query(table_input, output_reader),
        "q17" => q17::query(table_input, output_reader),
        "q18" => q18::query(table_input, output_reader),
        "q19" => q19::query(table_input, output_reader),
        "q2" => q2::query(table_input, output_reader),
        "q3" => q3::query(table_input, output_reader),
        "q6" => q6::query(table_input, output_reader),
        _ => panic!("Invalid Query Parameter"),
    };
    query_service
}