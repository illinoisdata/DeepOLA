use std::collections::hash_map::DefaultHasher;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::hash::Hasher;
use std::str::FromStr;
use std::str;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum DataType {
    Boolean,
    UnsignedInt,
    Integer,
    Float,
    Text,
    Tuple,
    Null,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum DataCell {
    Boolean(bool),
    UnsignedInt(usize),
    Integer(i32),
    Float(f64),
    Text(Box<str>),
    Tuple(Box<DataCell>,Box<DataCell>),
    Null(),
}

impl DataCell {
    // Hasher for a single DataCell
    pub fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        match self {
            DataCell::Boolean(a) => hasher.write_u8(*a as u8),
            DataCell::UnsignedInt(a) => hasher.write_usize(*a),
            DataCell::Integer(a) => hasher.write_i32(*a),
            DataCell::Float(_a) => {}
            DataCell::Text(a) => hasher.write(a.as_bytes()),
            DataCell::Tuple(a,b) => {
                hasher.write_u64(a.hash());
                hasher.write_u64(b.hash());
            },
            _ => {}
        }
        hasher.finish()
    }

    // Currently doesn't support hashing for f64 fields.
    // It can be supported by converting f64 into its (exponent, mantissa) form and hashing three of them.
    // Hasher for a vector of DataCell
    pub fn vector_hash(cells: Vec<DataCell>) -> u64 {
        let mut hasher = DefaultHasher::new();
        for cell in cells {
            match cell {
                DataCell::Boolean(a) => hasher.write_u8(a as u8),
                DataCell::UnsignedInt(a) => hasher.write_usize(a),
                DataCell::Integer(a) => hasher.write_i32(a),
                DataCell::Float(_a) => {}
                DataCell::Text(a) => hasher.write(a.as_bytes()),
                DataCell::Tuple(a,b) => {
                    hasher.write_u64(a.hash());
                    hasher.write_u64(b.hash());
                },
                _ => {}
            }
        }
        hasher.finish()
    }

    pub fn sum(cells: &[DataCell]) -> DataCell {
        if cells.is_empty() {
            return DataCell::Null();
        }
        match cells[0] {
            DataCell::Integer(a) => {
                let mut result = a;
                for cell in cells.iter().skip(1) {
                    result += i32::from(cell);
                }
                DataCell::Integer(result)
            },
            DataCell::Float(a) => {
                let mut result = a;
                for cell in cells.iter().skip(1) {
                    result += f64::from(cell);
                }
                DataCell::Float(result)
            }
            _ => panic!("SUM not implemented"),
        }
    }

    pub fn count(cells: &[DataCell]) -> DataCell {
        let mut result = 0;
        for cell in cells {
            match cell {
                DataCell::Null() => {
                    result += 0;
                },
                _ => {
                    result += 1;
                }
            }
        }
        DataCell::Integer(result)
    }

    pub fn avg(cells: &[DataCell]) -> DataCell {
        let sum = Self::sum(cells);
        let count = Self::count(cells);
        DataCell::Tuple(Box::new(sum), Box::new(count))
    }
}

// The PartialEq traits have been implemented to support direct comparison
// between DataCell and a value of the corresponding data type (i32, f64, str).
// We do not needed custom traits if we create a DataCell::<DataType>() instance and use that.
impl PartialEq<String> for DataCell {
    fn eq(&self, other: &String) -> bool {
        match self {
            DataCell::Text(a) => (a.as_ref() == other),
            _ => false,
        }
    }
}

impl PartialEq<&str> for DataCell {
    fn eq(&self, other: &&str) -> bool {
        match self {
            DataCell::Text(a) => (a.as_ref() == *other),
            _ => false,
        }
    }
}

impl PartialEq<i32> for DataCell {
    fn eq(&self, other: &i32) -> bool {
        match self {
            DataCell::Integer(a) => (a == other),
            DataCell::Float(a) => (*a == f64::from(*other)),
            _ => false,
        }
    }
}

impl PartialEq<f64> for DataCell {
    fn eq(&self, other: &f64) -> bool {
        match self {
            DataCell::Integer(a) => (f64::from(*a) == *other),
            DataCell::Float(a) => (a == other),
            _ => false,
        }
    }
}

impl DataCell {
    // Convert the String value to the specified data type.
    // Returns a DataValue object with the specified value.
    // This function would be called for each field in the rows that are read.
    pub fn create_data_cell(value: String, d: &DataType) -> Result<DataCell, Box<dyn Error>> {
        if value.chars().count() == 0 {
            return Ok(DataCell::Null());
        }
        match d {
            DataType::Boolean => Ok(DataCell::Boolean(value.parse::<bool>().unwrap())),
            DataType::UnsignedInt => Ok(DataCell::UnsignedInt(value.parse::<usize>().unwrap())),
            DataType::Integer => Ok(DataCell::Integer(value.parse::<i32>().unwrap())),
            DataType::Float => Ok(DataCell::Float(value.parse::<f64>().unwrap())),
            DataType::Text => Ok(DataCell::from(value)),
            _ => Err("Invalid Conversion Method".into()),
        }
    }

    pub fn create_data_cell_from_bytes(value: &[u8], d: &DataType) -> Result<DataCell, Box<dyn Error>> {
        if value.is_empty() {
            return Ok(DataCell::Null());
        }
        let parsed_value = unsafe {str::from_utf8_unchecked(value).to_string()};
        match d {
            DataType::Boolean => Ok(DataCell::Boolean(parsed_value.parse::<bool>().unwrap())),
            DataType::UnsignedInt => Ok(DataCell::UnsignedInt(parsed_value.parse::<usize>().unwrap())),
            DataType::Integer => Ok(DataCell::Integer(parsed_value.parse::<i32>().unwrap())),
            DataType::Float => Ok(DataCell::Float(parsed_value.parse::<f64>().unwrap())),
            DataType::Text => Ok(DataCell::from(parsed_value)),
            _ => Err("Invalid Conversion Method".into()),
        }
    }

    pub fn dtype(&self) -> DataType {
        match self {
            DataCell::Boolean(_a) => DataType::Boolean,
            DataCell::UnsignedInt(_a) => DataType::UnsignedInt,
            DataCell::Integer(_a) => DataType::Integer,
            DataCell::Float(_a) => DataType::Float,
            DataCell::Text(_a) => DataType::Text,
            DataCell::Tuple(_a, _b) => DataType::Tuple,
            DataCell::Null() => DataType::Null,
        }
    }
}

impl FromStr for DataCell {
    type Err = Infallible;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(DataCell::Text(value.into()))
    }
}

impl fmt::Display for DataCell {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let result = match self {
            DataCell::Boolean(a) => a.to_string(),
            DataCell::Integer(a) => a.to_string(),
            DataCell::Float(a) => a.to_string(),
            DataCell::Text(a) => a.to_string(),
            DataCell::Tuple(a,b) => format!("({},{})",a.to_string(),b.to_string()),
            _ => panic!("Invalid DataCell"),
        };
        write!(f, "{}", result)
    }
}

impl From<DataCell> for i32 {
    fn from(value: DataCell) -> Self {
        i32::from(&value)
    }
}

impl From<&DataCell> for i32 {
    fn from(value: &DataCell) -> Self {
        match value {
            DataCell::Integer(a) => *a,
            DataCell::Float(a) => *a as i32,
            _ => panic!("Invalid Conversion"),
        }
    }
}

impl From<DataCell> for f64 {
    fn from(value: DataCell) -> Self {
        f64::from(&value)
    }
}

impl From<&DataCell> for f64 {
    fn from(value: &DataCell) -> Self {
        match value {
            DataCell::Float(a) => *a,
            DataCell::Integer(a) => f64::from(*a),
            _ => panic!("Invalid Conversion"),
        }
    }
}

impl From<&str> for DataCell {
    fn from(value: &str) -> Self {
        DataCell::Text(value.into())
    }
}

impl From<&String> for DataCell {
    fn from(value: &String) -> Self {
        DataCell::Text(value.clone().into_boxed_str())
    }
}

impl From<String> for DataCell {
    fn from(value: String) -> Self {
        DataCell::Text(value.into_boxed_str())
    }
}

impl From<i32> for DataCell {
    fn from(value: i32) -> Self {
        DataCell::Integer(value)
    }
}

impl From<f64> for DataCell {
    fn from(value: f64) -> Self {
        DataCell::Float(value)
    }
}

impl From<usize> for DataCell {
    fn from(value: usize) -> Self {
        DataCell::Integer(value.try_into().unwrap())
    }
}

impl From<(DataCell, DataCell)> for DataCell {
    fn from(value: (DataCell, DataCell)) -> Self {
        DataCell::Tuple(Box::new(value.0),Box::new(value.1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_create_from_string() {
        let d = DataCell::from("hello");
        assert_eq!(d, "hello".to_string());
        assert_ne!(d, "world".to_string());
    }

    #[test]
    fn can_create_from_str() {
        let d = DataCell::from("hello");
        assert_eq!(d, "hello".to_string());
        assert_eq!(d, "hello");
        assert_ne!(d, "world".to_string());
        assert_ne!(d, "world");
    }

    #[test]
    fn can_create_from_float() {
        let d = DataCell::Float(1.0);
        assert_eq!(d, 1.0);
        assert_ne!(d, 2.0);
        assert_eq!(d, 1);
    }

    #[test]
    fn can_create_from_int() {
        let d = DataCell::Integer(1);
        assert_eq!(d, 1);
        assert_ne!(d, 2);
        assert_eq!(d, 1.0);
    }

    #[test]
    fn can_add_datacell() {
        let p = DataCell::Integer(1);
        let q = DataCell::Integer(2);
        let r = DataCell::Integer(3);
        assert_eq!(p + q, r);
    }

    #[test]
    fn can_sum_datacells() {
        let mut cells = vec![];
        let mut target_sum = 0;
        for i in 1..10 {
            cells.push(DataCell::Integer(i));
            target_sum += i;
        }
        assert_eq!(DataCell::sum(&cells), DataCell::Integer(target_sum));
    }

    #[test]
    fn can_ct_datacells() {
        let mut cells = vec![];
        let mut target_ct = 0;
        for i in 1..10 {
            cells.push(DataCell::Integer(i));
            target_ct += 1;
            if i % 2 == 0 {
                cells.push(DataCell::Null());
            }
        }
        assert_eq!(DataCell::count(&cells), DataCell::Integer(target_ct));
    }

    #[test]
    fn can_avg_datacells() {
        let mut cells = vec![];
        let mut target_sum = 0;
        let mut target_ct = 0;
        for i in 1..10 {
            cells.push(DataCell::Integer(i));
            target_sum += i;
            target_ct += 1;
        }
        assert_eq!(
            DataCell::avg(&cells),
            DataCell::Tuple(Box::new(DataCell::from(target_sum)), Box::new(DataCell::from(target_ct)))
        );
    }

    #[test]
    fn can_hash_datacell() {
        let cell1 = DataCell::Integer(1);
        let cell2 = DataCell::Integer(1);
        assert_eq!(cell1.hash(), cell2.hash());
        assert_eq!(
            DataCell::vector_hash(vec![DataCell::Integer(1), DataCell::from("hello")]),
            DataCell::vector_hash(vec![DataCell::Integer(1), DataCell::from("hello")])
        );
        assert_ne!(
            DataCell::vector_hash(vec![DataCell::Integer(1), DataCell::from("hello")]),
            DataCell::vector_hash(vec![DataCell::Integer(1), DataCell::from("hello ")])
        );
    }
}
