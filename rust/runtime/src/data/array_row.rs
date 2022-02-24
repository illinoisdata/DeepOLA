use std::ops::{Index, IndexMut};

use crate::data::data_type::DataCell;
use getset::Getters;

#[derive(Getters, Debug, Clone, PartialEq)]
pub struct ArrayRow {
    pub values: Vec<DataCell>,
}

impl ArrayRow {
    pub fn from_vector(values: Vec<DataCell>) -> ArrayRow {
        ArrayRow { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<Vec<DataCell>> for ArrayRow {
    fn from(values: Vec<DataCell>) -> Self {
        ArrayRow { values }
    }
}

impl From<&[DataCell]> for ArrayRow {
    fn from(array: &[DataCell]) -> Self {
        ArrayRow {
            values: Vec::<DataCell>::from(array),
        }
    }
}

impl<const N: usize> From<[DataCell; N]> for ArrayRow {
    fn from(array: [DataCell; N]) -> Self {
        ArrayRow {
            values: Vec::from(array),
        }
    }
}

impl ArrayRow {
    pub fn from_example() -> Vec<ArrayRow> {
        let example_row_1 = ArrayRow::from_vector(vec![
            DataCell::Integer(0),
            DataCell::Float(0.1),
            DataCell::Text(String::from("value1")),
        ]);
        let example_row_2 = ArrayRow::from_vector(vec![
            DataCell::Integer(1),
            DataCell::Float(0.9),
            DataCell::Text(String::from("value2")),
        ]);
        vec![example_row_1, example_row_2]
    }
}

impl Index<usize> for ArrayRow {
    type Output = DataCell;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl IndexMut<usize> for ArrayRow {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

#[cfg(test)]
mod tests {
    use super::ArrayRow;

    #[test]
    fn can_create_array_row() {
        let array_row = ArrayRow::from_example();
        assert_eq!(array_row.len(), 2);
        assert_eq!(array_row[0].len(), 3);
    }
}
