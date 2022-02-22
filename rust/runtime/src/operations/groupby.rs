use super::*;
use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};
use std::collections::{HashMap};
use std::cell::{RefCell};

pub struct GroupByNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// perform GROUP BY operation.
impl GroupByNode {
    pub fn new(groupby_cols: Vec<String>, aggregates: Vec<Aggregate>) -> ExecutionNode<ArrayRow> {
        let data_processor = GroupByMapper::new_boxed(groupby_cols, aggregates);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }
}

pub struct GroupByMapper {
    groupby_cols: Vec<String>,
    aggregates: Vec<Aggregate>,

    // GroupByMetaStore denotes the meta information corresponding to the agg index.
    // RefCell to treat GroupByMapper as immutable
    // metastore: RefCell<HashMap<u64, Vec<GroupByMetaStore>>>,

    // group_vals stores the actual DataCell corresponding to group by columns.
    // RefCell to treat GroupByMapper as immutable
    stored_groupby_keys: RefCell<HashMap<u64, Vec<DataCell>>>,

    // Stores the current group by result.
    // RefCell to treat GroupByMapper as immutable
    stored_groupby_agg: RefCell<HashMap<u64, Vec<DataCell>>>,
}

impl GroupByMapper {
    pub fn build_output_schema(&self, input_schema: Schema) -> Schema {
        let mut output_columns = Vec::new();
        // Add the groupby_cols as key columns in the output schema
        for groupby_col in &self.groupby_cols {
            output_columns.push(
                Column::from_key_field(
                    groupby_col.clone(),
                    input_schema.dtype(groupby_col.clone())
                )
            );
        }

        // Add the aggregate cols as additional columns in the output schema
        for aggregate in &self.aggregates {
            output_columns.push(
                Column::from_field(
                    aggregate.name(),
                    aggregate.dtype(input_schema.dtype(aggregate.column.clone()))
                )
            );
        }
        Schema::new("unnamed".to_string(),output_columns)
    }

    pub fn new(groupby_cols: Vec<String>, aggregates: Vec<Aggregate>) -> GroupByMapper {
        GroupByMapper {groupby_cols, aggregates, stored_groupby_keys: RefCell::new(HashMap::new()), stored_groupby_agg: RefCell::new(HashMap::new())}
    }

    pub fn new_boxed(groupby_cols: Vec<String>, aggregates: Vec<Aggregate>) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(Self::new(groupby_cols, aggregates))
    }

    pub fn update_groupby_result(&self, current_groupby_result: HashMap<u64, Vec<DataCell>>) {
        // Merges and stores the result in stored_groupby_result
        let mut result_hashmap = self.stored_groupby_agg.borrow_mut();
        for (key, value) in current_groupby_result.iter() {
            if result_hashmap.contains_key(key) {
                // Merge the values.
                // I know the existing values.
                // I know the updated values.
                // For sum and count we can merge.
                // For average how do we merge?
                let old_values = &result_hashmap[key];
                let new_values = value.iter().enumerate().map(|(i,v)| v.clone() + &old_values[i]).collect::<Vec<DataCell>>();
                result_hashmap.insert(key.clone(), new_values);
            }
            else {
                result_hashmap.insert(key.clone(), value.clone());
            }
        }
    }

    pub fn build_output_records(&self) -> Vec<ArrayRow> {
        // Return Vec<ArrayRow> from the stored HashMap
        let result_hashmap = self.stored_groupby_agg.borrow();
        let mut records = Vec::new();
        for (key, value) in result_hashmap.iter() {
            let mut record: Vec<DataCell> = Vec::new();
            let groupby_keys = &self.stored_groupby_keys.borrow()[key];
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
    fn process_v1<'a>(&'a self, input_set: &'a DataBlock<ArrayRow>) -> Generator<'a, (), DataBlock<ArrayRow>> {
        Gn::new_scoped(
            move |mut s| {
                let input_schema = input_set.metadata().get(SCHEMA_META_NAME).unwrap().to_schema();
                let metadata = HashMap::from([
                    (SCHEMA_META_NAME.into(), MetaCell::Schema(self.build_output_schema(input_schema.clone()))),
                    (DATABLOCK_TYPE.into(), MetaCell::Text(DATABLOCK_TYPE_DM.into())),
                ]);

                // key_indexes: Indexes corresponding to group by cols.
                // val_indexes: Indexes corresponding to aggregate cols.
                // col_indexes: Concatentation of the above two indexes.
                let key_indexes = self.groupby_cols.iter().map(|a| input_schema.index(a.to_string())).collect::<Vec<usize>>();
                let val_indexes = self.aggregates.iter().map(|a| input_schema.index(a.column.to_string())).collect::<Vec<usize>>();
                let mut col_indexes: Vec<usize> = vec![];
                col_indexes.extend(&key_indexes);
                col_indexes.extend(&val_indexes);

                // let grouped = items.iter().group_by(|&item| item.0).collect::<Vec<_>>();
                // Filter columns which are not used in the GROUP BY.
                // Compute GROUP BY results on current set of records.

                let mut local_hashmap: HashMap<u64, Vec<Vec<DataCell>> > = HashMap::new();

                for record in input_set.data().iter() {
                    let key = key_indexes.iter().map(|a| record[*a].clone()).collect::<Vec<DataCell>>();
                    let key_hash = DataCell::vector_hash(key.clone());

                    self.stored_groupby_keys.borrow_mut().entry(key_hash).or_insert(key);

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
                    let aggs = val_indexes.iter().enumerate().map(|(_i,a)| record[*a].clone());
                    let hash_entry = local_hashmap.get_mut(&key_hash).unwrap();
                    for (index, agg) in aggs.into_iter().enumerate() {
                        hash_entry[index].push(agg);
                    }
                }

                // Compute aggregates on local hashmap.
                let mut local_groupby_result: HashMap<u64, Vec<DataCell>> = HashMap::new();
                for (k,v) in local_hashmap {
                    let mut key_result = vec![];
                    for (i,cell) in v.iter().enumerate() {
                        key_result.push(self.aggregates[i].evaluate(cell));
                    }
                    local_groupby_result.insert(k, key_result);
                }
                // println!("Result {:?}",local_groupby_result);

                self.update_groupby_result(local_groupby_result);
                let output_records = self.build_output_records();
                let message = DataBlock::new(output_records, metadata);
                s.yield_(message);
                done!();
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_output_schema() {
        let input_schema = Schema::from_example("lineitem").unwrap();
        let groupby_cols = vec!["l_orderkey".to_string(),"l_partkey".to_string()];
        let aggregates = vec![
            Aggregate{
                column: "l_quantity".to_string(),
                operation:AggregationOperation::Sum,
                alias: None,
            }];
        let groupby_mapper = GroupByMapper::new(groupby_cols.clone(), aggregates.clone());
        // Output schema needs input payload schema.
        let output_schema = groupby_mapper.build_output_schema(input_schema.clone());

        let mut output_schema_cols = vec![];
        for col in groupby_cols {
            output_schema_cols.push(Column::from_key_field(col.clone(), input_schema.dtype(col.clone())));
        }
        output_schema_cols.push(Column::from_field("sum_l_quantity".to_string(), DataType::Integer));
        assert_eq!(output_schema, Schema::from(output_schema_cols));
    }

    #[test]
    fn get_output_schema_with_alias() {
        let input_schema = Schema::from_example("lineitem").unwrap();
        let groupby_cols = vec!["l_orderkey".to_string(),"l_partkey".to_string()];
        let aggregates = vec![
            Aggregate{
                column: "l_quantity".to_string(),
                operation:AggregationOperation::Sum,
                alias: Some("custom_sum_l_quantity".to_string()),
            }];
        let groupby_mapper = GroupByMapper::new(groupby_cols.clone(), aggregates.clone());
        let output_schema = groupby_mapper.build_output_schema(input_schema.clone());

        let mut output_schema_cols = vec![];
        for col in groupby_cols {
            output_schema_cols.push(Column::from_key_field(col.clone(), input_schema.dtype(col.clone())));
        }
        output_schema_cols.push(Column::from_field("custom_sum_l_quantity".to_string(), DataType::Integer));
        assert_eq!(output_schema, Schema::from(output_schema_cols));
    }

    #[test]
    fn test_groupby_node() {
        let arrayrow_messages = super::csvreader::get_example_arrayrow_messages();
        let groupby_cols = vec!["l_orderkey".to_string()];
        // ,"l_partkey".to_string()];
        let aggregates = vec![
            Aggregate{
                column: "l_quantity".to_string(),
                operation:AggregationOperation::Sum,
                alias: Some("custom_sum_l_quantity".to_string()),
            }];
        let groupby_node = GroupByNode::new(groupby_cols, aggregates);
        for message in arrayrow_messages {
            groupby_node.write_to_self(0, message);
        }
        groupby_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&groupby_node);
        groupby_node.run();

        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            println!("{:?}",dblock.data());
        }
    }
}