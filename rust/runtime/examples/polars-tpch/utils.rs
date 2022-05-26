extern crate runtime;
use runtime::data::*;
use std::error::Error;

pub struct TableInput {
    pub batch_size: usize,
    pub input_files: Vec<String>,
    pub scale: usize
}

pub fn _total_number_of_records(table: &str, scale: usize) -> usize {
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

pub fn tpch_schema(table: &str) -> Result<Schema, Box<dyn Error>> {
    let columns = match table {
        "lineitem" => vec![
            Column::from_key_field("l_orderkey".to_string(), DataType::Integer),
            Column::from_field("l_partkey".to_string(), DataType::Integer),
            Column::from_field("l_suppkey".to_string(), DataType::Integer),
            Column::from_key_field("l_linenumber".to_string(), DataType::Integer),
            Column::from_field("l_quantity".to_string(), DataType::Integer),
            Column::from_field("l_extendedprice".to_string(), DataType::Float),
            Column::from_field("l_discount".to_string(), DataType::Float),
            Column::from_field("l_tax".to_string(), DataType::Float),
            Column::from_field("l_returnflag".to_string(), DataType::Text),
            Column::from_field("l_linestatus".to_string(), DataType::Text),
            Column::from_field("l_shipdate".to_string(), DataType::Text),
            Column::from_field("l_commitdate".to_string(), DataType::Text),
            Column::from_field("l_receiptdate".to_string(), DataType::Text),
            Column::from_field("l_shipinstruct".to_string(), DataType::Text),
            Column::from_field("l_shipmode".to_string(), DataType::Text),
            Column::from_field("l_comment".to_string(), DataType::Text),
        ],
        "orders" => vec![
            Column::from_key_field("o_orderkey".to_string(), DataType::Integer),
            Column::from_field("o_custkey".to_string(), DataType::Integer),
            Column::from_field("o_orderstatus".to_string(), DataType::Text),
            Column::from_field("o_totalprice".to_string(), DataType::Float),
            Column::from_field("o_orderdate".to_string(), DataType::Text),
            Column::from_field("o_orderpriority".to_string(), DataType::Text),
            Column::from_field("o_clerk".to_string(), DataType::Text),
            Column::from_field("o_shippriority".to_string(), DataType::Integer),
            Column::from_field("o_comment".to_string(), DataType::Text),
        ],
        "customer" => vec![
            Column::from_key_field("c_custkey".to_string(), DataType::Integer),
            Column::from_field("c_name".to_string(), DataType::Text),
            Column::from_field("c_address".to_string(), DataType::Text),
            Column::from_field("c_nationkey".to_string(), DataType::Integer),
            Column::from_field("c_phone".to_string(), DataType::Text),
            Column::from_field("c_acctbal".to_string(), DataType::Float),
            Column::from_field("c_mktsegment".to_string(), DataType::Text),
            Column::from_field("c_comment".to_string(), DataType::Text),
        ],
        "supplier" => vec![
            Column::from_key_field("s_suppkey".to_string(), DataType::Integer),
            Column::from_field("s_name".to_string(), DataType::Text),
            Column::from_field("s_address".to_string(), DataType::Text),
            Column::from_field("s_nationkey".to_string(), DataType::Integer),
            Column::from_field("s_phone".to_string(), DataType::Text),
            Column::from_field("s_acctbal".to_string(), DataType::Float),
            Column::from_field("s_comment".to_string(), DataType::Text),
        ],
        "nation" => vec![
            Column::from_key_field("n_nationkey".to_string(), DataType::Integer),
            Column::from_field("n_name".to_string(), DataType::Text),
            Column::from_field("n_regionkey".to_string(), DataType::Integer),
            Column::from_field("n_comment".to_string(), DataType::Text),
        ],
        "region" => vec![
            Column::from_key_field("r_regionkey".to_string(), DataType::Integer),
            Column::from_field("r_name".to_string(), DataType::Text),
            Column::from_field("r_comment".to_string(), DataType::Text),
        ],
        "part" => vec![
            Column::from_key_field("p_partkey".to_string(), DataType::Integer),
            Column::from_field("p_name".to_string(), DataType::Text),
            Column::from_field("p_mfgr".to_string(), DataType::Text),
            Column::from_field("p_brand".to_string(), DataType::Text),
            Column::from_field("p_type".to_string(), DataType::Text),
            Column::from_field("p_size".to_string(), DataType::Integer),
            Column::from_field("p_container".to_string(), DataType::Text),
            Column::from_field("p_retailprice".to_string(), DataType::Float),
            Column::from_field("p_comment".to_string(), DataType::Text),
        ],
        "partsupp" => vec![
            Column::from_key_field("ps_partkey".to_string(), DataType::Integer),
            Column::from_key_field("ps_suppkey".to_string(), DataType::Integer),
            Column::from_field("ps_availqty".to_string(), DataType::Integer),
            Column::from_field("ps_supplycost".to_string(), DataType::Float),
            Column::from_field("ps_comment".to_string(), DataType::Text),
        ],
        _ => vec![],
    };
    if columns.is_empty() {
        Err("Schema Not Defined".into())
    } else {
        Ok(Schema::new(
            String::from(table),
            columns.clone()
        ))
    }
}