use crate::data::data_type::DataType;
use std::error::Error;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: &'static str,
    pub dtype: DataType
}

impl Column {
    pub fn from_field(name: &'static str, dtype: DataType) -> Column {
        Column {
            name,
            dtype
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub table: String,
    pub columns: Vec<Column>,
    _column_index: HashMap<&'static str,usize>
}

// Implemented this trait to be able to reference columns in tests with column names.
impl Schema {
    pub fn new(table: String, columns: Vec<Column>) -> Schema {
        let _column_index = columns.iter().enumerate().map(|(i, x)| (x.name.clone(), i)).collect();
        Schema {
            table,
            columns,
            _column_index
        }
    }

    pub fn index(&self, column: &str) -> usize {
        match self._column_index.get(column) {
            Some(index) => index.clone(),
            None => panic!("Invalid column name")
        }
    }

    pub fn from_example(table: &str) -> Result<Schema, Box<dyn Error>> {
        match table {
            "lineitem" => Ok(Schema::new(
                String::from(table),
                vec![
                    Column::from_field("l_orderkey",DataType::Integer),
                    Column::from_field("l_partkey",DataType::Integer),
                    Column::from_field("l_suppkey",DataType::Integer),
                    Column::from_field("l_linenumber",DataType::Integer),
                    Column::from_field("l_quantity",DataType::Integer),
                    Column::from_field("l_extendedprice",DataType::Float),
                    Column::from_field("l_discount",DataType::Float),
                    Column::from_field("l_tax",DataType::Float),
                    Column::from_field("l_returnflag",DataType::Text),
                    Column::from_field("l_linestatus",DataType::Text),
                    // For Date type fields, implement additional datatype called Date which is stored as usize.
                    Column::from_field("l_shipdate",DataType::Text),
                    Column::from_field("l_commitdate",DataType::Text),
                    Column::from_field("l_receiptdate",DataType::Text),
                    Column::from_field("l_shipinstruct",DataType::Text),
                    Column::from_field("l_shipmode",DataType::Text),
                    Column::from_field("l_comment",DataType::Text)
                ]
            )),
            "test_arraydata" => Ok(Schema::new(
                String::from(table),
                vec![
                    Column::from_field("col1",DataType::Integer),
                    Column::from_field("col2",DataType::Text),
                    Column::from_field("col3",DataType::Text),
                    Column::from_field("col4",DataType::Integer)
                ]
            )),
            _ => return Err("Schema Not Defined".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;

    #[test]
    fn can_create_schema_object() {
        let schema = Schema::from_example("lineitem").unwrap();
        assert_eq!(schema.columns.len(),16);
        assert_eq!(schema.index("l_orderkey"),0);
        assert_eq!(schema.index("l_suppkey"),2);
        assert_eq!(schema.index("l_comment"),15);
    }
}
