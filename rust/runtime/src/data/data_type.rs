use std::error::Error;
use crate::data::schema::Schema;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum DataType {
    Boolean,
    UnsignedInt,
    Integer,
    Float,
    Text,
    Schema
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum DataCell {
    Boolean(bool),
    UnsignedInt(usize),
    Integer(i32),
    Float(f64),
    Text(String),
    Schema(Schema),
}

// The PartialEq traits have been implemented to support direct comparison
// between DataCell and a value of the corresponding data type (i32, f64, str).
// We do not needed custom traits if we create a DataCell::<DataType>() instance and use that.
impl PartialEq<String> for DataCell {
    fn eq(&self, other: &String) -> bool {
        match self {
            DataCell::Text(a) => (a == other),
            _ => false
        }
    }
}

impl PartialEq<&str> for DataCell {
    fn eq(&self, other: &&str) -> bool {
        match self {
            DataCell::Text(a) => (a == other),
            _ => false
        }
    }
}

impl PartialEq<i32> for DataCell {
    fn eq(&self, other: &i32) -> bool {
        match self {
            DataCell::Integer(a) => (a == other),
            DataCell::Float(a) => (*a == f64::from(*other)),
            _ => false
        }
    }
}

impl PartialEq<f64> for DataCell {
    fn eq(&self, other: &f64) -> bool {
        match self {
            DataCell::Integer(a) => (f64::from(*a) == *other),
            DataCell::Float(a) => (a == other),
            _ => false
        }
    }
}

impl DataCell {
    // Convert the String value to the specified data type.
    // Returns a DataValue object with the specified value.
    // This function would be called for each field in the rows that are read.
    pub fn create_data_cell(value: String, d: &DataType) -> Result<DataCell, Box<dyn Error> > {
        match d {
            DataType::Boolean => Ok(DataCell::Boolean(value.parse::<bool>().unwrap())),
            DataType::UnsignedInt => Ok(DataCell::UnsignedInt(value.parse::<usize>().unwrap())),
            DataType::Integer => Ok(DataCell::Integer(value.parse::<i32>().unwrap())),
            DataType::Float => Ok(DataCell::Float(value.parse::<f64>().unwrap())),
            DataType::Text => Ok(DataCell::Text(value)),
            _ => Err("Invalid Conversion Method")?
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            DataCell::Boolean(a) => a.to_string(),
            DataCell::Integer(a) => a.to_string(),
            DataCell::Float(a) => a.to_string(),
            DataCell::Text(a) => a.to_string(),
            _ => panic!("Invalid DataCell")
        }
    }

    pub fn from_str(value: &str) -> Self {
        DataCell::Text(value.to_string())
    }
}

impl From<&str> for DataCell {
    fn from(value: &str) -> Self {
        DataCell::Text(value.to_string())
    }
}

impl From<i32> for DataCell {
    fn from(value: i32) -> Self {
        DataCell::Integer(value)
    }
}

impl From<usize> for DataCell {
    fn from(value: usize) -> Self {
        DataCell::Integer(value.try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::DataCell;


    #[test]
    fn can_create_from_string() {
        let d = DataCell::Text("hello".to_string());
        assert_eq!(d, "hello".to_string());
        assert_ne!(d, "world".to_string());
    }

    #[test]
    fn can_create_from_str() {
        let d = DataCell::from_str("hello");
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

}
