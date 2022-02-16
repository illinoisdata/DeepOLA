use crate::data::data_type::DataType;
use std::error::Error;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub dtype: DataType,
    pub key: bool
}

impl Column {
    // Whether the column makes a part of the key?
    pub fn from_field(name: String, dtype: DataType) -> Column {
        Self::from_field_with_key(name, dtype, false)
    }

    pub fn from_key_field(name: String, dtype: DataType) -> Column {
        Self::from_field_with_key(name, dtype, true)
    }

    fn from_field_with_key(name: String, dtype: DataType, key: bool) -> Column {
        Column {
            name,
            dtype,
            key
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub table: String,
    pub columns: Vec<Column>,
    _column_index: HashMap<String,usize>,
    _key_index: Vec<usize>
}

impl Schema {
    pub fn new(table: String, columns: Vec<Column>) -> Schema {
        // Populate the column index based on the columns that are specified.
        let _column_index = columns.iter().enumerate().map(|(i, x)| (x.name.clone(), i)).collect();

        // Iterate over column and populate the _key_index.
        let _key_index = columns.iter().enumerate().filter(|(_,x)| x.key).map(|(i,_)| i).collect();
        Schema {
            table,
            columns,
            _column_index,
            _key_index
        }
    }

    pub fn keys(&self) -> &Vec<usize> {
        &self._key_index
    }

    // Implemented this function to be able to reference columns in tests with column names.
    pub fn index(&self, column: &str) -> usize {
        match self._column_index.get(column) {
            Some(index) => *index,
            None => panic!("Invalid column name")
        }
    }

    // Get Column object corresponding to the column name. 
    pub fn get_column(&self, column: &str) -> Column {
        self.columns[self.index(column)].clone()
    }

    // Function to create and test examples of Schema creation
    pub fn from_example(table: &str) -> Result<Schema, Box<dyn Error>> {
        match table {
            "lineitem" => Ok(Schema::new(
                String::from(table),
                vec![
                    Column::from_field("l_orderkey".to_string(),DataType::Integer),
                    Column::from_field("l_partkey".to_string(),DataType::Integer),
                    Column::from_field("l_suppkey".to_string(),DataType::Integer),
                    Column::from_field("l_linenumber".to_string(),DataType::Integer),
                    Column::from_field("l_quantity".to_string(),DataType::Integer),
                    Column::from_field("l_extendedprice".to_string(),DataType::Float),
                    Column::from_field("l_discount".to_string(),DataType::Float),
                    Column::from_field("l_tax".to_string(),DataType::Float),
                    Column::from_field("l_returnflag".to_string(),DataType::Text),
                    Column::from_field("l_linestatus".to_string(),DataType::Text),
                    // For Date type fields, implement additional datatype called Date which is stored as usize.
                    Column::from_field("l_shipdate".to_string(),DataType::Text),
                    Column::from_field("l_commitdate".to_string(),DataType::Text),
                    Column::from_field("l_receiptdate".to_string(),DataType::Text),
                    Column::from_field("l_shipinstruct".to_string(),DataType::Text),
                    Column::from_field("l_shipmode".to_string(),DataType::Text),
                    Column::from_field("l_comment".to_string(),DataType::Text)
                ]
            )),
            "test_arraydata" => Ok(Schema::new(
                String::from(table),
                vec![
                    Column::from_field("col1".to_string(),DataType::Integer),
                    Column::from_field("col2".to_string(),DataType::Text),
                    Column::from_field("col3".to_string(),DataType::Text),
                    Column::from_field("col4".to_string(),DataType::Integer)
                ]
            )),
            _ => Err("Schema Not Defined".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;
    use super::Column;
    use crate::data::data_type::DataType;

    #[test]
    fn can_create_schema_object() {
        let schema = Schema::from_example("lineitem").unwrap();
        assert_eq!(schema.columns.len(),16);
        assert_eq!(schema.index("l_orderkey"),0);
        assert_eq!(schema.index("l_suppkey"),2);
        assert_eq!(schema.index("l_comment"),15);
    }

    #[test]
    fn can_create_schema_object_with_single_key() {
        let schema = Schema::new(
            String::from("test_schema_key"),
            vec![
                Column::from_key_field("col1".to_string(),DataType::Integer),
                Column::from_field("col2".to_string(),DataType::Text),
                Column::from_field("col3".to_string(),DataType::Text),
                Column::from_field("col4".to_string(),DataType::Integer)
            ]
        );
        assert_eq!(schema.keys(), &vec![0usize]);
    }

    #[test]
    fn can_create_schema_object_with_multiple_keys() {
        let schema = Schema::new(
            String::from("test_schema_key"),
            vec![
                Column::from_field("col1".to_string(),DataType::Integer),
                Column::from_key_field("col2".to_string(),DataType::Text),
                Column::from_key_field("col3".to_string(),DataType::Text),
                Column::from_field("col4".to_string(),DataType::Integer)
            ]
        );
        assert_eq!(schema.keys(), &vec![1usize,2]);
    }
}
