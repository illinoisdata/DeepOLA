use crate::data::*;

#[derive(Debug, Clone)]
pub enum AggregationOperation {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    CountDistinct,
    SumDistinct,
    AvgDistinct,
}

impl ToString for AggregationOperation {
    fn to_string(&self) -> String {
        match self {
            AggregationOperation::Sum => "sum".to_string(),
            AggregationOperation::Avg => "avg".to_string(),
            AggregationOperation::Count => "count".to_string(),
            AggregationOperation::Min => "min".to_string(),
            AggregationOperation::Max => "max".to_string(),
            AggregationOperation::CountDistinct => "count_distinct".to_string(),
            AggregationOperation::SumDistinct => "sum_distinct".to_string(),
            AggregationOperation::AvgDistinct => "avg_distinct".to_string(),
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
            AggregationOperation::Min => col_dtype,
            AggregationOperation::Max => col_dtype,
            AggregationOperation::CountDistinct => DataType::Integer,
            AggregationOperation::SumDistinct => col_dtype,
            AggregationOperation::AvgDistinct => DataType::Float,
        }
    }

    // Evaluate aggregation operation on a vector of DataCell
    pub fn evaluate(&self, values: &[DataCell]) -> DataCell {
        match self.operation {
            AggregationOperation::Sum => DataCell::sum(values),
            AggregationOperation::Count => DataCell::count(values),
            AggregationOperation::Avg => DataCell::from((DataCell::sum(values),DataCell::count(values))),
            AggregationOperation::Min => DataCell::min(values),
            AggregationOperation::Max => DataCell::max(values),
            _ => panic!("Aggregation Operation Not Implemented"),
        }
    }

    pub fn merge(&self, old_value: DataCell, new_value: &DataCell) -> DataCell {
        match self.operation {
            AggregationOperation::Sum => old_value + new_value,
            AggregationOperation::Count => old_value + new_value,
            AggregationOperation::Avg => match old_value {
                DataCell::Tuple(old_values) => match new_value {
                    DataCell::Tuple(new_values) => {
                        DataCell::Tuple(
                            Box::new((
                                (*old_values).0 + (*new_values).clone().0,
                                (*old_values).1 + (*new_values).clone().1
                            ))
                        )
                    },
                    _ => panic!("Invalid new_value DataCell for AVG operation")
                },
                _ => panic!("Invalid old_value DataCell for AVG operation")
            },
            AggregationOperation::Min => {
                if old_value < *new_value {
                    old_value
                } else {
                    new_value.clone()
                }
            },
            AggregationOperation::Max => {
                if old_value < *new_value {
                    new_value.clone()
                } else {
                    old_value
                }
            },
            _ => panic!("Invalid Aggregation Operation"),
        }
    }
}
