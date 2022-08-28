# Install dev tools
Compiler: https://doc.rust-lang.org/cargo/getting-started/installation.html

VS Code (IDE): https://code.visualstudio.com/

# Running examples:
To run the queries in `examples/tpch_polars/`, run `RUST_LOG=info cargo run --release --example tpch_polars -- q1`. The default scale and directory used to run the query is `resources/tpc-h/data/scale=1/partition=1/`. To specify a different directory, run:
`RUST_LOG=info cargo run --release --example tpch_polars -- q<query-no> <scale> <directory>`

For example:
`RUST_LOG=info cargo run --release --example tpch_polars -- q1 1 resources/tpc-h/data/scale=1/partition=10/`

# Directory Structure
```
benches/ - Code for benchmarks written in `wake`.
examples/ - Code for various examples implemented using `wake`.
src/ - Code for wake.
resources/ - Link to the `resources` directory in the root folder of the repository.
```

# Implementing a new Query
To implement a new TPC-H query, create a file `q<query-no>.rs` in `examples/tpch_polars/`. Refer to `examples/tpch_polars/q1.rs` for the function to implement. The query would be executed from `examples/tpch_polars/main.rs`. Make sure, to import the query in `main.rs`, using `mod q<query-no>` and add a mapping from string number to `query` function of the tpc-h query.