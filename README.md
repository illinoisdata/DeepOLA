# DeepOLA: Online Aggregation for Nested Queries
Online Aggregation (OLA) is a technique that incrementally improves the query result estimates allowing the user to observe the query progress as well as control its execution on the fly. OLA provides the user with an approximate estimate of the query result as soon as it has processed a small partition of the data. With DeepOLA, we intend to speed-up approximate (as well as actual) query computation when the available data is divided into various chunks that can be processed online and merged to obtain the complete result.

## Setup Instructions
DeepOLA is implemented in Rust. The current implementation has been tested with `rustc 1.60.0`. You can install Rust using https://www.rust-lang.org/tools/install. Once you have Rust installed, follow the following instructions to setup the repository.
- Clone the repository
`git clone https://github.com/illinoisdata/cs511_p3template; cd DeepOLA/`
- Make sure the pre-generated TPC-H data is stored in `resources/tpc-h/data/` directory (`scale=1/partition=10/*.tbl` files).
- To run the provided example queries, from `deepola/wake`, run `cargo run --release --example tpch_polars -- query q<query-no>`. Example: `cargo run --release --example tpch_polars -- query q1`.

