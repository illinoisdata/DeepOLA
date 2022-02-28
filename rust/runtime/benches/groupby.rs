use criterion::{criterion_group, criterion_main};
use criterion::BenchmarkId;
use criterion::Criterion;

use runtime::data::*;
use runtime::operations::{GroupByNode, Aggregate, AggregationOperation};
use runtime::graph::ExecutionNode;
use std::collections::HashMap;

static RECORD_SIZE: usize = 1000000;
static NUM_GROUP_KEYS: usize = 5;
static NUM_VAL_COLS: usize = 5;

fn generate_groupby_arrayrow(num_groups: usize, num_group_keys: usize, num_val_cols: usize) -> Vec<ArrayRow> {
    // Create vector of ArrayRows
    // Unique groups of size num_group_keys
    // Generate random values

    // Create num_groups vectors of num_group_keys dimension
    let mut groups = vec![];
    for i in 0..num_groups {
        let mut group_row = vec![];
        for j in 0..num_group_keys {
            group_row.push(DataCell::from(format!("group{}col{}", i, j)));
        }
        groups.push(group_row);
    }
    let mut result = vec![];
    for i in 0..RECORD_SIZE {
        let mut record = vec![];
        // Choose a random group in range 0..num_groups
        // Add group by keys based on this chosen group
        let chosen_group = i%num_groups;
        for group_key_cell in &groups[chosen_group] {
            record.push(group_key_cell.clone());
        }
        // Add value in each aggregate row
        for j in 0..num_val_cols{
            record.push(DataCell::Integer( (i+j) as i32));
        }
        result.push(ArrayRow::from_vector(record));
    }
    result
}

fn get_schema(num_group_keys: usize, num_val_cols: usize) -> Schema {
    let mut cols = vec![];
    for i in 0..num_group_keys {
        cols.push(Column::from_key_field(format!("key{}", i),DataType::Text));
    }
    for i in 0..num_val_cols {
        cols.push(Column::from_field(format!("val{}", i),DataType::Text));
    }
    Schema::new("unnamed".to_string(), cols)
}

fn get_groupby_node(num_group_keys: usize, num_val_cols: usize) -> ExecutionNode<ArrayRow> {
    // Generate groupby cols names "key1", "key2", "key3", ....
    let mut groupby_cols = vec![];
    for groupby_key in 0..num_group_keys {
        groupby_cols.push(format!("key{}", groupby_key))
    }

    // Generate aggregate cols names "val1", "val2", "val3", ....
    // Run SUM operation on these.
    let mut aggregates = vec![];
    for val_col in 0..num_val_cols {
        aggregates.push(
            Aggregate {
                column: format!("val{}", val_col),
                operation: AggregationOperation::Sum,
                alias: None,
            }
        );
    }
    let groupby_node = GroupByNode::node(groupby_cols, aggregates);
    groupby_node
}

fn groupby_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("GROUP BY Operation");
    for num_groups in [1, 100, 10000, 1000000].iter() {
        group.sample_size(10);
        group.bench_with_input(BenchmarkId::from_parameter(num_groups), num_groups, |b, &num_groups| {
            let groupby_node = get_groupby_node(NUM_GROUP_KEYS, NUM_VAL_COLS);
            let arrayrow_records = generate_groupby_arrayrow(num_groups, NUM_GROUP_KEYS, NUM_VAL_COLS);
            let metadata = HashMap::from([
                (SCHEMA_META_NAME.into(),MetaCell::Schema(get_schema(NUM_GROUP_KEYS, NUM_VAL_COLS))),
                (DATABLOCK_TYPE.into(),MetaCell::Text(DATABLOCK_TYPE_DM.into())),
            ]);
            let dblock = 
                DataBlock::new(
                    arrayrow_records,
                    metadata
                );
            b.iter(|| {
                let arrayrow_message = DataMessage::from(dblock.clone());
                groupby_node.write_to_self(0, arrayrow_message);
                groupby_node.write_to_self(0, DataMessage::eof());
                groupby_node.run();
            });
        });
    }
    group.finish();
}

criterion_group!(groupby_benches,
    groupby_node
);

criterion_main!(groupby_benches);