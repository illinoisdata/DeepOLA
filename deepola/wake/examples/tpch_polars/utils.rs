extern crate wake;
use polars::prelude::*;
use wake::data::*;
use wake::graph::*;
use wake::polars_operations::*;

use glob::glob;
use std::collections::HashMap;
use std::error::Error;
use std::time::Instant;

#[derive(Debug)]
pub struct TableInput {
    pub input_files: Vec<String>,
    pub scale: usize,
}

pub fn total_number_of_records(table: &str, scale: usize) -> usize {
    match table {
        "lineitem" => 6_000_000 * scale,
        "orders" => 1_500_000 * scale,
        "part" => 200_000 * scale,
        "partsupp" => 800_000 * scale,
        "customer" => 150_000 * scale,
        "supplier" => 10_000 * scale,
        "nation" => 25,
        "region" => 5,
        _ => 0,
    }
}

pub fn load_tables(directory: &str, scale: usize) -> HashMap<String, TableInput> {
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

pub fn run_query(
    query_service: &mut ExecutionService<DataFrame>,
    output_reader: &mut NodeReader<DataFrame>,
) -> Vec<DataFrame> {
    let mut query_result: Vec<DataFrame> = vec![];
    let start_time = Instant::now();
    query_service.run();
    loop {
        let message = output_reader.read();
        if message.is_eof() {
            break;
        }
        let data = message.datablock().data();
        query_result.push(data.clone());
    }
    query_service.join();
    let end_time = Instant::now();
    log::info!("Query Result");
    log::info!("{:?}", query_result[query_result.len() - 1]);
    log::info!("Query Took: {:.2?}", end_time - start_time);
    query_result
}

pub fn build_csv_reader_node(
    table: String,
    tableinput: &HashMap<String, TableInput>,
    table_columns: &HashMap<String, Vec<&str>>,
) -> ExecutionNode<polars::prelude::DataFrame> {
    // Get batch size and file names from tableinput tables;
    let raw_input_files = tableinput.get(&table as &str).unwrap().input_files.clone();
    let scale = tableinput.get(&table as &str).unwrap().scale.clone();
    let schema = tpch_schema(&table).unwrap();

    let projected_cols_index = match table_columns.get(&table) {
        Some(columns) => Some(columns.iter().map(|x| schema.index(x)).collect()),
        None => None, // No columns specified means taking all columns.
    };
    let projected_column_names = match table_columns.get(&table) {
        Some(columns) => Some(columns.iter().map(|x| x.to_string()).collect()),
        None => None,
    };

    let input_files = df!("col" => &raw_input_files).unwrap();

    let csvreader = CSVReaderBuilder::new()
        .delimiter('|')
        .has_headers(false)
        .column_names(projected_column_names)
        .projected_cols(projected_cols_index)
        .build();

    let mut metadata = MetaCell::Schema(schema.clone()).into_meta_map();
    *metadata
        .entry(DATABLOCK_TOTAL_RECORDS.to_string())
        .or_insert(MetaCell::Float(0.0)) =
        MetaCell::Float(total_number_of_records(&table, scale) as f64);

    let dblock = DataBlock::new(input_files.clone(), metadata);
    csvreader.write_to_self(0, DataMessage::from(dblock));
    csvreader.write_to_self(0, DataMessage::eof());
    csvreader
}

pub fn tpch_schema(table: &str) -> std::result::Result<wake::data::Schema, Box<dyn Error>> {
    let columns = match table {
        "lineitem" => vec![
            Column::from_key_field("l_orderkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("l_partkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("l_suppkey".to_string(), wake::data::DataType::Integer),
            Column::from_key_field("l_linenumber".to_string(), wake::data::DataType::Integer),
            Column::from_field("l_quantity".to_string(), wake::data::DataType::Integer),
            Column::from_field("l_extendedprice".to_string(), wake::data::DataType::Float),
            Column::from_field("l_discount".to_string(), wake::data::DataType::Float),
            Column::from_field("l_tax".to_string(), wake::data::DataType::Float),
            Column::from_field("l_returnflag".to_string(), wake::data::DataType::Text),
            Column::from_field("l_linestatus".to_string(), wake::data::DataType::Text),
            Column::from_field("l_shipdate".to_string(), wake::data::DataType::Text),
            Column::from_field("l_commitdate".to_string(), wake::data::DataType::Text),
            Column::from_field("l_receiptdate".to_string(), wake::data::DataType::Text),
            Column::from_field("l_shipinstruct".to_string(), wake::data::DataType::Text),
            Column::from_field("l_shipmode".to_string(), wake::data::DataType::Text),
            Column::from_field("l_comment".to_string(), wake::data::DataType::Text),
        ],
        "orders" => vec![
            Column::from_key_field("o_orderkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("o_custkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("o_orderstatus".to_string(), wake::data::DataType::Text),
            Column::from_field("o_totalprice".to_string(), wake::data::DataType::Float),
            Column::from_field("o_orderdate".to_string(), wake::data::DataType::Text),
            Column::from_field("o_orderpriority".to_string(), wake::data::DataType::Text),
            Column::from_field("o_clerk".to_string(), wake::data::DataType::Text),
            Column::from_field("o_shippriority".to_string(), wake::data::DataType::Integer),
            Column::from_field("o_comment".to_string(), wake::data::DataType::Text),
        ],
        "customer" => vec![
            Column::from_key_field("c_custkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("c_name".to_string(), wake::data::DataType::Text),
            Column::from_field("c_address".to_string(), wake::data::DataType::Text),
            Column::from_field("c_nationkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("c_phone".to_string(), wake::data::DataType::Text),
            Column::from_field("c_acctbal".to_string(), wake::data::DataType::Float),
            Column::from_field("c_mktsegment".to_string(), wake::data::DataType::Text),
            Column::from_field("c_comment".to_string(), wake::data::DataType::Text),
        ],
        "supplier" => vec![
            Column::from_key_field("s_suppkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("s_name".to_string(), wake::data::DataType::Text),
            Column::from_field("s_address".to_string(), wake::data::DataType::Text),
            Column::from_field("s_nationkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("s_phone".to_string(), wake::data::DataType::Text),
            Column::from_field("s_acctbal".to_string(), wake::data::DataType::Float),
            Column::from_field("s_comment".to_string(), wake::data::DataType::Text),
        ],
        "nation" => vec![
            Column::from_key_field("n_nationkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("n_name".to_string(), wake::data::DataType::Text),
            Column::from_field("n_regionkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("n_comment".to_string(), wake::data::DataType::Text),
        ],
        "region" => vec![
            Column::from_key_field("r_regionkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("r_name".to_string(), wake::data::DataType::Text),
            Column::from_field("r_comment".to_string(), wake::data::DataType::Text),
        ],
        "part" => vec![
            Column::from_key_field("p_partkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("p_name".to_string(), wake::data::DataType::Text),
            Column::from_field("p_mfgr".to_string(), wake::data::DataType::Text),
            Column::from_field("p_brand".to_string(), wake::data::DataType::Text),
            Column::from_field("p_type".to_string(), wake::data::DataType::Text),
            Column::from_field("p_size".to_string(), wake::data::DataType::Integer),
            Column::from_field("p_container".to_string(), wake::data::DataType::Text),
            Column::from_field("p_retailprice".to_string(), wake::data::DataType::Float),
            Column::from_field("p_comment".to_string(), wake::data::DataType::Text),
        ],
        "partsupp" => vec![
            Column::from_key_field("ps_partkey".to_string(), wake::data::DataType::Integer),
            Column::from_key_field("ps_suppkey".to_string(), wake::data::DataType::Integer),
            Column::from_field("ps_availqty".to_string(), wake::data::DataType::Integer),
            Column::from_field("ps_supplycost".to_string(), wake::data::DataType::Float),
            Column::from_field("ps_comment".to_string(), wake::data::DataType::Text),
        ],
        _ => vec![],
    };
    if columns.is_empty() {
        Err("Schema Not Defined".into())
    } else {
        Ok(wake::data::Schema::new(
            String::from(table),
            columns.clone(),
        ))
    }
}