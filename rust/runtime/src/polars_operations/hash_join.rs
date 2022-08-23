use polars::prelude::*;
use polars::frame::hash_join::create_probe_table;
use polars::frame::hash_join::{get_splitted_chunked_array, create_hash_table_information};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_join_by_reusing_hash_table() {
        let filename = "src/resources/lineitem-100.csv";
        let reader1 = polars::prelude::CsvReader::from_path(filename)
            .unwrap()
            .has_header(false)
            .with_delimiter(',' as u8);
        let df1 = reader1.finish().unwrap();

        let reader2 = polars::prelude::CsvReader::from_path(filename)
            .unwrap()
            .has_header(false)
            .with_delimiter(',' as u8);
        let df2 = reader2.finish().unwrap();

        let splitted_chunked_array = get_splitted_chunked_array(&df2, vec!["column_1".to_string()]);
        let hash_table_information = create_hash_table_information(&splitted_chunked_array);
        let result_df = df1.join(&df2, vec!["column_1"], vec!["column_1"], JoinType::Inner, None, Some(hash_table_information));
        println!("{:?}", result_df);
    }
}
