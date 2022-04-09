use super::*;
use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use rustc_hash::FxHashMap;

pub struct GroupByNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// perform GROUP BY operation.
impl GroupByNode {
    pub fn node(groupby_cols: Vec<String>, aggregates: Vec<Aggregate>) -> ExecutionNode<ArrayRow> {
        let data_processor = GroupByMapper::new_boxed(groupby_cols, aggregates);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }
}

pub struct GroupByMapper {
    groupby_cols: Vec<String>,
    aggregates: Vec<Aggregate>,

    // group_vals stores the actual DataCell corresponding to group by columns.
    // RefCell to treat GroupByMapper as immutable
    stored_groupby_keys: RefCell<FxHashMap<u64, Vec<DataCell>>>,

    // Stores the current result of the group by node.
    // RefCell to treat GroupByMapper as immutable
    stored_groupby_agg: RefCell<FxHashMap<u64, Vec<DataCell>>>,

    // Output record length
    record_length: usize,
}

impl GroupByMapper {
    // Builds the output schema based on the input schema, group by cols and aggregates
    pub fn build_output_schema(&self, input_schema: Schema) -> Schema {
        let mut output_columns = Vec::new();
        // Add the groupby_cols as key columns in the output schema
        for groupby_col in &self.groupby_cols {
            output_columns.push(Column::from_key_field(
                groupby_col.clone(),
                input_schema.dtype(groupby_col.clone()),
            ));
        }

        // Add the aggregate cols as additional columns in the output schema
        for aggregate in &self.aggregates {
            output_columns.push(Column::from_field(
                aggregate.name(),
                aggregate.dtype(input_schema.dtype(aggregate.column.clone())),
            ));
        }
        Schema::new("unnamed".to_string(), output_columns)
    }

    pub fn new(groupby_cols: Vec<String>, aggregates: Vec<Aggregate>) -> GroupByMapper {
        let record_length = groupby_cols.len() + aggregates.len();
        GroupByMapper {
            groupby_cols,
            aggregates,
            stored_groupby_keys: RefCell::new(FxHashMap::default()),
            stored_groupby_agg: RefCell::new(FxHashMap::default()),
            record_length
        }
    }

    pub fn new_boxed(
        groupby_cols: Vec<String>,
        aggregates: Vec<Aggregate>,
    ) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(Self::new(groupby_cols, aggregates))
    }

    // Merges the current_groupby_result with the stored_groupby_result
    pub fn update_groupby_result(&self, current_groupby_result: HashMap<u64, Vec<DataCell>>) {
        let mut result_hashmap = self.stored_groupby_agg.borrow_mut();
        for (key, value) in current_groupby_result.into_iter() {
            if result_hashmap.contains_key(&key) {
                // Key exist. Update the value.
                // Currently supports only SUM and COUNT operation.
                let old_values = &result_hashmap[&key];
                let new_values = value
                    .iter()
                    .enumerate()
                    .map(|(i, v)| self.aggregates[i].merge(v.clone(),&old_values[i])) // Here I need to know the aggregate.
                    .collect::<Vec<DataCell>>();
                result_hashmap.insert(key, new_values);
            } else {
                // Key doesn't exist. Insert the key.
                result_hashmap.insert(key, value);
            }
        }
    }

    pub fn build_output_records(&self) -> Vec<ArrayRow> {
        let groupby_col_hashmap = self.stored_groupby_keys.borrow();
        let agg_val_hashmap = self.stored_groupby_agg.borrow();
        self._build_output_records(groupby_col_hashmap, agg_val_hashmap)
    }

    // Uses the stored_groupby_key to obtain the group by columns.
    // Uses the stored_groupby_agg to obtain the aggregate columns.
    // Return Vec<ArrayRow> from the stored HashMap
    fn _build_output_records(
        &self,
        groupby_col_hashmap: Ref<FxHashMap<u64, Vec<DataCell>>>,
        agg_val_hashmap: Ref<FxHashMap<u64, Vec<DataCell>>>
    ) -> Vec<ArrayRow> {
        let mut records = Vec::new();
        for (key, value) in agg_val_hashmap.iter() {
            let mut record: Vec<DataCell> = Vec::with_capacity(self.record_length);
            let groupby_keys = &groupby_col_hashmap[key];
            let agg_values = value;
            for col in groupby_keys {
                record.push(col.clone())
            }
            for col in agg_values {
                record.push(col.clone())
            }
            records.push(ArrayRow::from_vector(record));
        }
        records
    }
}

impl SetProcessorV1<ArrayRow> for GroupByMapper {
    fn process_v1<'a>(
        &'a self,
        input_set: &'a DataBlock<ArrayRow>,
    ) -> Generator<'a, (), DataBlock<ArrayRow>> {
        Gn::new_scoped(move |mut s| {
            // Build output schema metadata
            let input_schema = input_set
                .metadata()
                .get(SCHEMA_META_NAME)
                .unwrap()
                .to_schema();
            let metadata = HashMap::from([
                (
                    SCHEMA_META_NAME.into(),
                    MetaCell::Schema(self.build_output_schema(input_schema.clone())),
                ),
                (
                    DATABLOCK_TYPE.into(),
                    MetaCell::Text(DATABLOCK_TYPE_DM.into()),
                ),
            ]);

            // key_indexes: Indexes corresponding to group by cols.
            // val_indexes: Indexes corresponding to aggregate cols.
            // col_indexes: Concatentation of the above two indexes.
            let key_indexes = self
                .groupby_cols
                .iter()
                .map(|a| input_schema.index(a.to_string()))
                .collect::<Vec<usize>>();
            let val_indexes = self
                .aggregates
                .iter()
                .map(|a| input_schema.index(a.column.to_string()))
                .collect::<Vec<usize>>();
            let mut col_indexes: Vec<usize> = vec![];
            col_indexes.extend(&key_indexes);
            col_indexes.extend(&val_indexes);

            // HashMap to first group rows and collect by group by keys
            let mut local_hashmap: HashMap<u64, Vec<Vec<DataCell>>> = HashMap::new();

            // For each record, identify the group it belongs to.
            // Add the columns corresponding to aggregate to a Vec in the local_hashmap
            for record in input_set.data().iter() {
                let key = key_indexes
                    .iter()
                    .map(|a| record[*a].clone())
                    .collect::<Vec<DataCell>>();
                let key_hash = DataCell::vector_hash(key.clone());
                self.stored_groupby_keys
                    .borrow_mut()
                    .entry(key_hash)
                    .or_insert(key);

                // If key not in local_hashmap
                // Create an empty vector to hold each aggregate information.
                local_hashmap.entry(key_hash).or_insert_with(|| {
                    let mut empty_vector = Vec::with_capacity(val_indexes.len());
                    for _i in 0..val_indexes.len() {
                        empty_vector.push(Vec::new());
                    }
                    empty_vector
                });

                // In the local_hashmap, add the aggs for each of the values.
                let aggs = val_indexes
                    .iter()
                    .enumerate()
                    .map(|(_i, a)| record[*a].clone());
                let hash_entry = local_hashmap.get_mut(&key_hash).unwrap();
                for (index, agg) in aggs.into_iter().enumerate() {
                    hash_entry[index].push(agg);
                }
            }

            // Evaluate aggregates on the local_hashmap.
            let mut local_groupby_result: HashMap<u64, Vec<DataCell>> = HashMap::new();
            for (k, v) in local_hashmap {
                let mut key_result = vec![];
                for (i, cell) in v.iter().enumerate() {
                    key_result.push(self.aggregates[i].evaluate(cell));
                }
                local_groupby_result.insert(k, key_result);
            }

            // Update the group by result with the newly computed result
            self.update_groupby_result(local_groupby_result);

            // Build the output dataframe
            let output_records = self.build_output_records();
            let message = DataBlock::new(output_records, metadata);
            s.yield_(message);
            done!();
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_output_schema() {
        let input_schema = Schema::from_example("lineitem").unwrap();
        let groupby_cols = vec!["l_orderkey".to_string(), "l_partkey".to_string()];
        let aggregates = vec![Aggregate {
            column: "l_quantity".to_string(),
            operation: AggregationOperation::Sum,
            alias: None,
        }];
        let groupby_mapper = GroupByMapper::new(groupby_cols.clone(), aggregates.clone());
        let output_schema = groupby_mapper.build_output_schema(input_schema.clone());

        let tgt_output_schema = Schema::from(vec![
            Column::from_key_field("l_orderkey".into(), input_schema.dtype("l_orderkey".into())),
            Column::from_key_field("l_partkey".into(), input_schema.dtype("l_partkey".into())),
            Column::from_field("sum_l_quantity".into(), DataType::Integer),
        ]);
        assert_eq!(output_schema, tgt_output_schema);
    }

    #[test]
    fn get_output_schema_with_alias() {
        let input_schema = Schema::from_example("lineitem").unwrap();
        let groupby_cols = vec!["l_orderkey".to_string(), "l_partkey".to_string()];
        let aggregates = vec![Aggregate {
            column: "l_quantity".to_string(),
            operation: AggregationOperation::Sum,
            alias: Some("custom_sum_l_quantity".to_string()),
        }];
        let groupby_mapper = GroupByMapper::new(groupby_cols.clone(), aggregates.clone());
        let output_schema = groupby_mapper.build_output_schema(input_schema.clone());

        let tgt_output_schema = Schema::from(vec![
            Column::from_key_field("l_orderkey".into(), input_schema.dtype("l_orderkey".into())),
            Column::from_key_field("l_partkey".into(), input_schema.dtype("l_partkey".into())),
            Column::from_field("custom_sum_l_quantity".into(), DataType::Integer),
        ]);
        assert_eq!(output_schema, tgt_output_schema);
    }

    #[test]
    fn test_groupby_node_one_key_one_agg() {
        let arrayrow_message = utils::example_city_arrow_message();
        let groupby_cols = vec!["country".to_string()];
        let aggregates = vec![Aggregate {
            column: "population".to_string(),
            operation: AggregationOperation::Sum,
            alias: None,
        }];
        let groupby_node = GroupByNode::node(groupby_cols, aggregates);
        groupby_node.write_to_self(0, arrayrow_message);
        groupby_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&groupby_node);
        groupby_node.run();

        let message = reader_node.read();
        let data = message.datablock().data();
        assert_eq!(data.len(), 2);
        for row in data {
            assert!(
                (row[0] == DataCell::Text("US".into()) && row[1] == 1000)
                    || (row[0] == DataCell::Text("IN".into()) && row[1] == 2600)
            );
        }
    }

    #[test]
    fn test_groupby_node_one_key_multi_agg() {
        let arrayrow_message = utils::example_city_arrow_message();
        let groupby_cols = vec!["country".to_string()];
        let aggregates = vec![
            Aggregate {
                column: "population".to_string(),
                operation: AggregationOperation::Sum,
                alias: None,
            },
            Aggregate {
                column: "area".to_string(),
                operation: AggregationOperation::Count,
                alias: None,
            },
        ];
        let groupby_node = GroupByNode::node(groupby_cols, aggregates);
        groupby_node.write_to_self(0, arrayrow_message);
        groupby_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&groupby_node);
        groupby_node.run();

        let message = reader_node.read();
        let data = message.datablock().data();
        assert_eq!(data.len(), 2);
        for row in data {
            assert!(
                (row[0] == DataCell::Text("US".into()) && row[1] == 1000 && row[2] == 3)
                    || (row[0] == DataCell::Text("IN".into()) && row[1] == 2600 && row[2] == 2)
            )
        }
    }

    #[test]
    fn test_groupby_node_multi_key() {
        let arrayrow_message = utils::example_city_arrow_message();
        let groupby_cols = vec!["country".to_string(), "state".to_string()];
        let aggregates = vec![Aggregate {
            column: "population".to_string(),
            operation: AggregationOperation::Sum,
            alias: None,
        }];
        let groupby_node = GroupByNode::node(groupby_cols, aggregates);
        groupby_node.write_to_self(0, arrayrow_message);
        groupby_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&groupby_node);
        groupby_node.run();

        let message = reader_node.read();
        let data = message.datablock().data();
        assert_eq!(data.len(), 4);
        assert_eq!(data[0].len(), 3);
    }
}
