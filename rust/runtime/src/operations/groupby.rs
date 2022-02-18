use crate::processor::SetProcessor;
use crate::data::schema::{Schema,Column};
use crate::data::payload::DataBlock;

#[derive(Debug, Clone)]
pub enum AggregationOperation {
    Sum,
    Avg,
    Count,
    CountDistinct,
}

#[derive(Debug, Clone)]
pub struct Expression {
    columns: &'static str
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    operation: AggregationOperation,
    column: Expression
}

pub struct GroupByMapper<T> {
    record_map: Box<dyn Fn(&T) -> Option<T>>, // Logic to map this record to an output record.
    input_schema: Schema,
    groupby_cols: Vec<&'static str>,
    aggregates: Vec<Aggregate>,
    output_schema: Option<Schema>, // Set based on the input schema and the mapper parameters.
}

unsafe impl<T> Send for GroupByMapper<T> {}

impl<T> SetProcessor<T> for GroupByMapper<T> {
    fn process(&self, input_set: &DataBlock<T>) -> DataBlock<T> {
        let mut records: Vec<T> = vec![];
        for r in input_set.data().iter() {
            match (self.record_map)(r) {
                Some(a) => records.push(a),
                None => (),
            }
        }
        DataBlock::from_records(records)
    }
}

impl<T> GroupByMapper<T> {
    fn _build_output_schema(&self) -> Option<Schema> {
        let mut output_columns = Vec::new();
        for groupby_col in &self.groupby_cols {
            output_columns.push(self.input_schema.get_column(groupby_col));
        }
        for aggregate in &self.aggregates {
            output_columns.push(self.input_schema.get_column(aggregate.column.columns));
        }
        Some(Schema::new("temp".to_string(),output_columns))
    }

    pub fn new(record_map: Box<dyn Fn(&T) -> Option<T>>, input_schema: Schema, groupby_cols: Vec<&'static str>, aggregates: Vec<Aggregate>) -> GroupByMapper<T> {
        let mut groupbymapper = GroupByMapper {
            record_map,
            input_schema,
            groupby_cols,
            aggregates,
            output_schema: None,
        };
        groupbymapper.output_schema = groupbymapper._build_output_schema();
        groupbymapper
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;
    use super::{Aggregate,AggregationOperation,Expression,GroupByMapper};

    #[test]
    fn can_get_output_schema() {
        let input_schema = Schema::from_example("lineitem").unwrap();
        let record_map = Box::new(|a: &AggregationOperation| {Some(AggregationOperation::Sum)});
        let groupby_cols = vec!["l_orderkey","l_partkey"];
        let aggregates = vec![Aggregate{column: Expression{columns:"l_quantity"}, operation:AggregationOperation::Sum}];
        let groupby_mapper = GroupByMapper::new(
            record_map,
            input_schema,
            groupby_cols.clone(),
            aggregates.clone()
        );
        let output_schema = groupby_mapper.output_schema.unwrap();
        println!("{:?}",output_schema);
        assert_eq!(output_schema.columns.len(),groupby_cols.len() + aggregates.len());
    }
}