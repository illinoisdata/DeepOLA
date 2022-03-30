use std::ops::{Index, IndexMut};
use std::fmt;

use crate::data::data_type::DataCell;
use getset::Getters;

#[derive(Getters, Debug, Clone, PartialEq)]
pub struct ArrayRow {
    pub values: Vec<DataCell>,
}

unsafe impl Send for ArrayRow {}

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

    pub fn slice_indices(&self, indices: &[usize]) -> Vec<DataCell> {
      indices.iter().map(|idx| self[*idx].clone()).collect()
    }
}

impl From<Vec<DataCell>> for ArrayRow {
    fn from(values: Vec<DataCell>) -> Self {
        ArrayRow { values }
    }
}

// Clone values from a vector of DataCell references
impl From<Vec<&DataCell>> for ArrayRow {
    fn from(values: Vec<&DataCell>) -> Self {
        ArrayRow { values: values.into_iter().cloned().collect() }
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
            DataCell::from("value1"),
        ]);
        let example_row_2 = ArrayRow::from_vector(vec![
            DataCell::Integer(1),
            DataCell::Float(0.9),
            DataCell::from("value2"),
        ]);
        vec![example_row_1, example_row_2]
    }
}

impl fmt::Display for ArrayRow {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for cell in &self.values {
            write!(f,"{} | ",cell).expect("Error displaying ArrayRow");
        }
        writeln!(f)
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

// TODO: Implement Iterator for ArrayRow that iterates over values.

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
