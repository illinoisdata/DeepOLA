use crate::data::DataCell;

pub enum PredicateOperation {
    GreaterThanEqualTo,
    LessThanEqualTo,
    GreaterThan,
    LessThan,
    EqualTo,
    NotEqualTo,
    // TODO: Between, In, Like
}

pub enum OperandType {
    StaticData,
    ColumnName
}

/// The `index` attribute should be initialized with the
/// index in the record array in case of ColumnName dtype
pub struct Operand {
    value: DataCell,
    index: Option<usize>,
    dtype: OperandType
}

/// Each Predicate is represented by multiple operands and a predicate operation.
/// Depending on the PredicateOperation, there are different requirements on the size of operands.
pub struct Predicate {
    operands: Vec<Operand>,
    operation: PredicateOperation
}

impl Predicate {
    fn new(operands: Vec<Operand>, operation: PredicateOperation) -> Predicate {
        Predicate { operands, operation}
    }

    fn evaluate(&self, record: &[DataCell]) -> bool {
        // Evaluate current predicate on given record.
        let left_operand = &self.operands[0];
        let right_operand = &self.operands[1];
        match self.operation {
            PredicateOperation::GreaterThanEqualTo => {
                match right_operand.dtype {
                    OperandType::StaticData => record[left_operand.index.unwrap()] >= right_operand.value,
                    OperandType::ColumnName => record[left_operand.index.unwrap()] >= record[right_operand.index.unwrap()],
                }
            },
            PredicateOperation::LessThanEqualTo => {
                match right_operand.dtype {
                    OperandType::StaticData => record[left_operand.index.unwrap()] <= right_operand.value,
                    OperandType::ColumnName => record[left_operand.index.unwrap()] <= record[right_operand.index.unwrap()],
                }
            },
            PredicateOperation::GreaterThan => {
                match right_operand.dtype {
                    OperandType::StaticData => record[left_operand.index.unwrap()] > right_operand.value,
                    OperandType::ColumnName => record[left_operand.index.unwrap()] > record[right_operand.index.unwrap()],
                }
            },
            PredicateOperation::LessThan => {
                match right_operand.dtype {
                    OperandType::StaticData => record[left_operand.index.unwrap()] < right_operand.value,
                    OperandType::ColumnName => record[left_operand.index.unwrap()] < record[right_operand.index.unwrap()],
                }
            },
            PredicateOperation::EqualTo => {
                match right_operand.dtype {
                    OperandType::StaticData => record[left_operand.index.unwrap()] == right_operand.value,
                    OperandType::ColumnName => record[left_operand.index.unwrap()] == record[right_operand.index.unwrap()],
                }
            },
            PredicateOperation::NotEqualTo => {
                match right_operand.dtype {
                    OperandType::StaticData => record[left_operand.index.unwrap()] != right_operand.value,
                    OperandType::ColumnName => record[left_operand.index.unwrap()] != record[right_operand.index.unwrap()],
                }
            },
            _ => { true }
        }
    }
}

impl Operand {
    fn new(value: DataCell, index: Option<usize>, dtype: OperandType) -> Operand {
        Operand { value, index, dtype}
    }

    fn from_column(value: &str, index: usize) -> Operand {
        Operand::new(DataCell::from(value), Some(index), OperandType::ColumnName)
    }

    fn from_static(value: DataCell) -> Operand {
        Operand::new(value, None, OperandType::StaticData)
    }
}


mod tests {
    use super::*;
    use crate::operations::utils;

    #[test]
    fn test_predicate_evaluation() {
        let operands = vec![
            Operand::from_column("population",3),
            Operand::from_static(DataCell::Integer(300))
        ];
        let operation = PredicateOperation::GreaterThanEqualTo;
        let predicate = Predicate::new(operands, operation);

        let records = utils::example_city_arrow_rows();
        let mut valid_count = 0;
        for record in records {
            let result = predicate.evaluate(&record.values);
            if result {
                valid_count += 1;
            }
        }
        assert_eq!(valid_count, 6);
    }
}
