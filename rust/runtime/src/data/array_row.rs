use std::ops::{Index, IndexMut};
use std::fmt;
use std::borrow::Cow;

use crate::data::data_type::DataCell;
use getset::Getters;

#[derive(Getters, Debug, Clone, PartialEq)]
pub struct ArrayRow {
    pub values: Vec<Cow<'static,DataCell>>,
}

unsafe impl Send for ArrayRow {}

impl ArrayRow {
    pub fn from_vector_cow(values: Vec<Cow<'static,DataCell>>) -> ArrayRow {
        ArrayRow { values }
    }

    pub fn from_vector(values: Vec<DataCell>) -> ArrayRow {
        let vector_cow = values.iter().map(|x| Cow::Owned(x.clone())).collect::<Vec<Cow<DataCell>>>();
        ArrayRow::from_vector_cow(vector_cow)
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
        ArrayRow::from_vector(values)
    }
}

// Clone values from a vector of DataCell references
impl From<Vec<&DataCell>> for ArrayRow {
    fn from(values: Vec<&DataCell>) -> Self {
        ArrayRow::from_vector(values.into_iter().cloned().collect())
    }
}

impl From<&[DataCell]> for ArrayRow {
    fn from(array: &[DataCell]) -> Self {
        ArrayRow::from_vector(Vec::<DataCell>::from(array))
    }
}

impl From<Vec<Cow<'static,DataCell>>> for ArrayRow {
    fn from(values: Vec<Cow<'static,DataCell>>) -> Self {
        ArrayRow::from_vector_cow(values)
    }
}

impl<const N: usize> From<[DataCell; N]> for ArrayRow {
    fn from(array: [DataCell; N]) -> Self {
        ArrayRow::from_vector(Vec::from(array))
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
        self.values[index].to_mut()
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
