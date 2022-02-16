use getset::Getters;
use crate::data::data_type::DataCell;

#[derive(Getters, Debug, Clone)]
pub struct ArrayRow {
    pub values: Vec<DataCell>
}

impl ArrayRow {
    pub fn from_vector(values: Vec<DataCell>) -> ArrayRow {
        ArrayRow { 
            values
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }
}

impl ArrayRow {
    pub fn from_example() -> Vec<ArrayRow> {
        let example_row_1 = ArrayRow::from_vector(vec![DataCell::Integer(0),DataCell::Float(0.1),DataCell::Text(String::from("value1"))]);
        let example_row_2 = ArrayRow::from_vector(vec![DataCell::Integer(1),DataCell::Float(0.9),DataCell::Text(String::from("value2"))]);
        vec![example_row_1, example_row_2]
    }
}

#[cfg(test)]
mod tests {
    use super::ArrayRow;

    #[test]
    fn can_create_array_row() {
        let array_row = ArrayRow::from_example();
        assert_eq!(array_row.len(),2);
        assert_eq!(array_row[0].len(),3);
    }
}