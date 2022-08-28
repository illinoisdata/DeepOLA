# DeepOLA: Online Aggregation for Nested Queries
Online Aggregation (OLA) is a technique that incrementally improves the query result estimates allowing the user to observe the query progress as well as control its execution on the fly. OLA provides the user with an approximate estimate of the query result as soon as it has processed a small partition of the data. With DeepOLA, we intend to speed-up approximate (as well as actual) query computation when the available data is divided into various chunks that can be processed online and merged to obtain the complete result.

## Setup Instructions
DeepOLA is implemented in Rust. The current implementation has been tested with `rustc 1.60.0`. You can install Rust using https://www.rust-lang.org/tools/install. Once you have Rust installed, follow the following instructions to setup the repository.
- Clone the repository
`git clone https://github.com/illinoisdata/DeepOLA; cd DeepOLA/`
- Make sure the pre-generated TPC-H data is also fetched in `resources/tpc-h/data/` directory (`lineitem_1M.tbl` and `scale=1/partition=1/*.tbl` files). If the above files are not present, fetch them using Git LFS.
`git lfs fetch`
- From the `deepola/` directory, run `cargo test` to run the already included test-cases.
- To run the provided example queries, run `cargo run --release --example tpch_polars -- q<query-no>`. Example:
`cargo run --release --example tpch_polars -- q1`.

## TPC-H Benchmark
TPC-H Benchmark is a decision-support benchmark. This benchmark illustrates decision support systems that examine large volumes of data, execute queries with a high degree of complexity, and give answers to critical business questions. For more information on the official benchmark, refer to [https://www.tpc.org/tpch/](https://www.tpc.org/tpch/). For generating data and queries from this benchmark, we use the `tpch-dbgen` kit available at [https://github.com/dragansah/tpch-dbgen](https://github.com/dragansah/tpch-dbgen)

To generate TPC-H dataset, go to `cd scripts/; ./data-gen.sh <scale> <num_partitions>`. For example: `./data-gen.sh 1 10` will generate a `scale=1` dataset divided into 10 partitions in the directory `resources/tpc-h/data/scale=1/partition=10/`.