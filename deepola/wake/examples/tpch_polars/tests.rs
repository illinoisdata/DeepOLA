use polars::{prelude::{DataFrame, NamedFrom}, series::Series};
use wake::graph::NodeReader;
use crate::utils;

fn get_expected_df(query_no: &str) -> DataFrame {
    match query_no {
        "q14" => DataFrame::new(vec![Series::new("promo_revenue", &vec![17.287581])]).unwrap(),
        _ => DataFrame::default()
    }
}

pub fn test_tpch_query(query_no: &str) {
    let scale = 1;
    let data_directory = "resources/tpc-h/data/scale=0.05/partition=5";
    let mut output_reader = NodeReader::empty();
    let mut query_service = crate::get_query_service(query_no, scale, data_directory, &mut output_reader);
    let result = utils::run_query(&mut query_service, &mut output_reader);
    let expected_df = get_expected_df(query_no);
    assert_eq!(result[result.len()-1], expected_df);
}