use crate::data::*;

#[derive(Debug, Clone)]
pub enum AggregationOperation {
    Sum,
    Avg,
    Count,
    CountDistinct,
}

impl ToString for AggregationOperation {
    fn to_string(&self) -> String {
        match self {
            AggregationOperation::Sum => "sum".to_string(),
            AggregationOperation::Avg => "avg".to_string(),
            AggregationOperation::Count => "count".to_string(),
            AggregationOperation::CountDistinct => "count_distinct".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    pub operation: AggregationOperation,
    pub column: String,
    pub alias: Option<String>,
}

impl Aggregate {
    // If an alias is provided, use that name.
    // Otherwise construct name as <aggregate>_<column>
    pub fn name(&self) -> String {
        match &self.alias {
            Some(a) => a.clone(),
            None => {
                let mut name = self.operation.to_string();
                name.push('_');
                name.push_str(&self.column);
                name
            }
        }
    }

    // Infer DataType of output column given the operation and the input column DataType
    pub fn dtype(&self, col_dtype: DataType) -> DataType {
        match self.operation {
            AggregationOperation::Sum => col_dtype,
            AggregationOperation::Avg => DataType::Float,
            AggregationOperation::Count => DataType::Integer,
            AggregationOperation::CountDistinct => DataType::Integer,
        }
    }

    // Evaluate aggregation operation on a vector of DataCell
    pub fn evaluate(&self, values: &[DataCell]) -> DataCell {
        match self.operation {
            AggregationOperation::Sum => DataCell::sum(values),
            AggregationOperation::Count => DataCell::count(values),
            AggregationOperation::Avg => panic!("AVG Not Implemented"),
            _ => panic!("Invalid Aggregation Operation"),
        }
    }
}
