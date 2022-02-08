use getset::Getters;
use std::error::Error;
use itertools::izip;

use crate::data::{schema::Schema,data_type::DataCell};

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
}

impl ArrayRow {
    pub fn from_example() -> Vec<ArrayRow> {
        let example_row_1 = ArrayRow::from_vector(vec![DataCell::Integer(0),DataCell::Float(0.1),DataCell::Text(String::from("value1"))]);
        let example_row_2 = ArrayRow::from_vector(vec![DataCell::Integer(1),DataCell::Float(0.9),DataCell::Text(String::from("value2"))]);
        vec![example_row_1, example_row_2]
    }
}

#[derive(Getters, Debug, Clone)]
pub struct ArrayData {
    // Column names for the dataframe.
    #[getset(get = "pub")]
    pub schema: Schema, 

    // This represent the actual rows in the dataframe.
    #[getset(get = "pub")]
    pub rows: Vec<ArrayRow>
}

impl ArrayData {
    // Currently assumes that the first row corresponds to header.
    // Can add a boolean header and optionally read the first row as header or data.
    pub fn from_csv(filename: &str, schema: &Schema) -> Result<ArrayData, Box<dyn Error> > {
        let mut reader = csv::Reader::from_path(filename)?;
        let mut records = Vec::new();
        for result in reader.records() {
            let record = result?;
            let mut data_cells = Vec::new();
            for (value,column) in izip!(&record,&schema.columns) {
                data_cells.push(DataCell::create_data_cell(value.to_string(), &column.dtype).unwrap());
            }
            records.push(ArrayRow::from_vector(data_cells));
        }
        Ok(ArrayData {
            schema: schema.clone(),
            rows: records
        })
    }

    pub fn read_lineitem_table(filename: &str) -> Result<ArrayData, Box<dyn Error> >  {
        let lineitem_schema = Schema::from_example("lineitem").unwrap();
        ArrayData::from_csv(filename, &lineitem_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::ArrayData;
    use crate::data::{schema::Schema,data_type::DataCell};

    #[test]
    fn can_read_test_arraydata() {
        let schema = Schema::from_example("test_arraydata").unwrap();
        let array_data = ArrayData::from_csv("src/resources/test_arraydata.csv",&schema).unwrap();
        assert_eq!(array_data.rows.len(), 3);
        assert_eq!(array_data.rows[0].values[schema.index("col2")],DataCell::Text("san francisco".to_string()));
        assert_eq!(array_data.rows[0].values[schema.index("col4")],DataCell::Integer(100));
    }

    #[test]
    fn can_read_lineitem_data() {
        let schema = Schema::from_example("lineitem").unwrap();
        let array_data = ArrayData::from_csv("src/resources/lineitem-100.csv",&schema).unwrap();
        assert_eq!(array_data.rows.len(), 99);
        assert_eq!(array_data.rows[0].values[schema.index("l_partkey")], DataCell::Integer(155190));
        assert_eq!(array_data.rows[0].values[schema.index("l_extendedprice")], DataCell::Float(21168.23));
        assert_eq!(array_data.rows[0].values[schema.index("l_returnflag")], DataCell::Text("N".to_string()));
        assert_eq!(array_data.rows[0].values[schema.index("l_shipdate")], DataCell::Text("1996-03-13".to_string()));

        // Incorrect Value test.
        assert_ne!(array_data.rows[0].values[schema.index("l_extendedprice")], DataCell::Float(21169.23));
    }

    #[test]
    fn can_read_lineitem_data_with_partialeq() {
        let schema = Schema::from_example("lineitem").unwrap();
        let array_data = ArrayData::from_csv("src/resources/lineitem-100.csv",&schema).unwrap();
        assert_eq!(array_data.rows.len(), 99);
        assert_eq!(array_data.rows[0].values[schema.index("l_partkey")], 155190);
        assert_eq!(array_data.rows[0].values[schema.index("l_extendedprice")], 21168.23);
        assert_eq!(array_data.rows[0].values[schema.index("l_returnflag")], "N");
        assert_eq!(array_data.rows[0].values[schema.index("l_shipdate")], "1996-03-13");
        // Incorrect Value test.
        assert_ne!(array_data.rows[0].values[schema.index("l_extendedprice")], 21169.23);
    }
}