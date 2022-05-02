use criterion::{criterion_group, criterion_main};
use runtime::data::DataCell;
use criterion::{Criterion, Throughput};
use std::path::Path;
use std::collections::HashMap;
use std::str;
use runtime::data::*;
use runtime::operations::*;

fn deepola_csvreader_lineitem(c: &mut Criterion) {
    let mut group = c.benchmark_group("CSVReaderNode Throughput (LineItem; 1 million rows)");
    let filename = format!("src/resources/tpc-h/lineitem_1M.tbl");
    let path = Path::new(&filename);
    if path.exists() {
        group.throughput(Throughput::Bytes(path.metadata().unwrap().len() as u64));
        group.sample_size(10);
        // Create a CSV Node with this scale
        let batch_size = 100_000;
        let input_vec = vec![
            ArrayRow::from_vector(vec![DataCell::from(filename.clone())])
        ];
        // Metadata for DataBlock
        let lineitem_schema = Schema::from_example("lineitem").unwrap();
        let mut metadata = MetaCell::Schema(lineitem_schema.clone()).into_meta_map();
        *metadata.entry(DATABLOCK_TOTAL_RECORDS.to_string()).or_insert(MetaCell::Float(0.0)) = MetaCell::Float(1_000_000.0);
        let dblock = DataBlock::new(input_vec, metadata);

        group.bench_function("deepola_csvreader", |b| {
            b.iter(|| {
                let csvreader = CSVReaderNode::new_with_params(batch_size, '|', false);
                csvreader.write_to_self(0, DataMessage::from(dblock.clone()));
                csvreader.write_to_self(0, DataMessage::eof());
                csvreader.run();
            });
        });
    } else {
        println!("File Not Found to Run Bench");
    }
    group.finish();
}

// This benchmark reads a raw CSV file and parses the utf-8 data as strings.
// The idea is to see the difference between this and the above benchmark to understand the
// additional time added due to DataCell processing and Message passing
fn raw_csvreader_lineitem(c: &mut Criterion) {
    let mut group = c.benchmark_group("Raw CSV Throughput (LineItem; 1 million rows)");
    let filename = format!("src/resources/tpc-h/lineitem_1M.tbl");
    let path = Path::new(&filename);
    if path.exists() {
        group.throughput(Throughput::Bytes(path.metadata().unwrap().len() as u64));
        group.sample_size(10);
        group.bench_function("default csv reader", |b| {
            b.iter(|| {
                let mut rdr =
                  csv::ReaderBuilder::new()
                      .delimiter(b'|')
                      .has_headers(false)
                      .from_path(filename.clone())
                      .unwrap();
                let mut record = csv::ByteRecord::new();
                while rdr.read_byte_record(&mut record).unwrap() {
                    let _count = record.len();
                    let mut row = vec![];
                    for cell in record.iter() {
                        row.push(unsafe {str::from_utf8_unchecked(cell)});
                    }
                }
            });
        });
    } else {
        println!("File Not Found to Run Bench");
    }
    group.finish();
}

criterion_group!(csvreader_benches,
    deepola_csvreader_lineitem,
    raw_csvreader_lineitem
);

criterion_main!(csvreader_benches);