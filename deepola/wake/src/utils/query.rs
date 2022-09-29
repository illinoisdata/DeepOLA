use polars::prelude::*;
use crate::graph::*;
use std::time::Instant;

#[derive(Debug)]
pub struct TableInput {
    pub input_files: Vec<String>,
    pub scale: usize,
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
        if query_result.is_empty() {
            let end_time = Instant::now();
            log::info!("First Query Result Took: {:.2?}", end_time - start_time);
        }
        query_result.push(data.clone());
    }
    query_service.join();
    let end_time = Instant::now();
    log::info!("Query Result");
    log::info!("{:?}", query_result[query_result.len() - 1]);
    log::info!("Query Took: {:.2?}", end_time - start_time);
    query_result
}