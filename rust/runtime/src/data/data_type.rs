use std::error::Error;

#[derive(Debug, Clone)]
pub enum DataType {
    Boolean,
    UnsignedInt,
    Integer,
    Float,
    Text
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum DataCell {
    Boolean(bool),
    UnsignedInt(usize),
    Integer(i32),
    Float(f64),
    Text(String)
}

// The PartialEq traits have been implemented to support direct comparison
// between DataCell and a value of the corresponding data type (i32, f64, str).
// We do not needed custom traits if we create a DataCell::<DataType>() instance and use that.
impl PartialEq<&str> for DataCell {
    fn eq(&self, other: &&str) -> bool {
        match self {
            DataCell::Text(a) => (a == other),
            _ => panic!("Invalid Data Type Comparison")
        }
    }
}
impl PartialEq<i32> for DataCell {
    fn eq(&self, other: &i32) -> bool {
        match self {
            DataCell::Integer(a) => (a == other),
            _ => panic!("Invalid Data Type Comparison")
        }
    }
}
impl PartialEq<f64> for DataCell {
    fn eq(&self, other: &f64) -> bool {
        match self {
            DataCell::Float(a) => (a == other),
            _ => panic!("Invalid Data Type Comparison")
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
            DataType::Text => Ok(DataCell::Text(value))
            // _ => Err("Invalid Data Type")?
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
}