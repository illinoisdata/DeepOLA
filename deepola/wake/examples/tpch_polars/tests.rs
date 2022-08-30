// DISCLAIMER: YOU SHOULD NOT CHANGE ANY PARTS OF THIS FILE.
use crate::utils;
use polars::prelude::CsvWriter;
use polars::prelude::SerWriter;
use polars::prelude::{DataFrame, DataType, SerReader};
use std::fs::File;
use wake::graph::NodeReader;
use wake::polars_operations::util;

pub fn _save_df_to_csv(df: &mut DataFrame, file_path: String) {
    let mut output_file: File = File::create(file_path).unwrap();
    CsvWriter::new(&mut output_file)
        .has_header(true)
        .finish(df)
        .unwrap();
}

pub fn truncate_numeric_cols(df: &mut DataFrame) {
    let float_cols = df
        .get_columns()
        .iter()
        .filter_map(|x| {
            if x.dtype().eq(&DataType::Float64) {
                Some(x.name().to_string())
            } else {
                None
            }
        })
        .collect::<Vec<String>>();
    for col in float_cols {
        util::truncate_df(df, col.as_str(), 4);
    }
}

pub fn get_expected_df(query_no: &str) -> DataFrame {
    let filename = format!("resources/cs511/expected-outputs/{}.csv", query_no);
    let reader = polars::prelude::CsvReader::from_path(filename)
        .unwrap()
        .has_header(true)
        .with_delimiter(',' as u8);
    reader.finish().unwrap()
}

pub fn test_tpch_query(query_no: &str) {
    let scale = 1;
    let data_directory = "resources/tpc-h/data/scale=0.05/partition=5";
    let mut output_reader = NodeReader::empty();
    let mut query_service =
        crate::get_query_service(query_no, scale, data_directory, &mut output_reader);
    let result = utils::run_query(&mut query_service, &mut output_reader);
    let size = result.len();
    let mut obtained_df = result[size - 1].clone();
    truncate_numeric_cols(&mut obtained_df);
    let expected_df = get_expected_df(query_no);

    let mut matches = true;
    for (left, right) in obtained_df
        .get_columns()
        .iter()
        .zip(expected_df.get_columns())
    {
        if !left.series_equal_missing(right) {
            matches = false;
            println!("The following series doesn't match.");
            println!("Left Dataframe Series: {:?}", left);
            println!("Right Dataframe Series: {:?}", right);
            break;
        }
    }
    assert!(matches);
    println!("Output for Query: {} matches the expected output", query_no);
}
