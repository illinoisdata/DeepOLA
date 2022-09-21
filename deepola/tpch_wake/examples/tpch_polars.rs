#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate tpch_wake;
use std::env;

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
        "query" => {
            tpch_wake::run_query_with_args(args.into_iter().skip(1).collect::<Vec<String>>())
        }
        _ => panic!(
            "Invalid Argument to the cargo run command.
        Run: `cargo run --release --example tpch_polars -- query q1` to run query q1.
        Run `cargo run --release --example tpch_polars -- test` to run test for query q1."
        ),
    };
}
