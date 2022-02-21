use std::collections::HashMap;

use super::{Schema, Column};


#[derive(Clone, Debug, PartialEq)]
pub enum MetaCell {
    Schema(Schema),
    Text(String),
}

impl MetaCell {
    pub fn to_schema(&self) -> &Schema {
        match self {
            MetaCell::Schema(a) => a,
            _ => panic!("Not a Valid Schema DataCell")
        }
    }

    pub fn into_meta_map(&self) -> HashMap<String, MetaCell> {
        HashMap::from([
            (SCHEMA_META_NAME.into(), self.clone())
        ])
    }
}

pub const SCHEMA_META_NAME: &str = "reserved.schema";

impl From<Schema> for MetaCell {
    fn from(schema: Schema) -> Self {
        MetaCell::Schema(schema.clone())
    }
}

impl From<Vec<Column>> for MetaCell {
    fn from(cols: Vec<Column>) -> Self {
        Self::from(Schema::from(cols))
    }
}