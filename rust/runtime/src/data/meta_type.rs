use std::collections::HashMap;

use super::{Schema, Column};

pub const SCHEMA_META_NAME: &str = "reserved.schema";
pub const DATABLOCK_TYPE: &str = "reserved.type";
pub const DATABLOCK_TYPE_DM: &str = "dm";
pub const DATABLOCK_TYPE_DA: &str = "da";

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
            (SCHEMA_META_NAME.into(), self.clone()),
            (DATABLOCK_TYPE.into(), MetaCell::from(DATABLOCK_TYPE_DA)),
        ])
    }

    pub fn into_dm_meta_map(&self) -> HashMap<String, MetaCell> {
        HashMap::from([
            (SCHEMA_META_NAME.into(), self.clone()),
            (DATABLOCK_TYPE.into(), MetaCell::from(DATABLOCK_TYPE_DM)),
        ])
    }
}

impl From<&str> for MetaCell {
    fn from(value: &str) -> Self {
        MetaCell::Text(value.to_string())
    }
}

impl From<MetaCell> for String {
    fn from(cell: MetaCell) -> Self {
        match cell {
            MetaCell::Text(a) => a,
            _ => panic!("Invalid conversion from MetaCell")
        }
    }
}

impl From<Schema> for MetaCell {
    fn from(schema: Schema) -> Self {
        MetaCell::Schema(schema)
    }
}

impl From<Vec<Column>> for MetaCell {
    fn from(cols: Vec<Column>) -> Self {
        Self::from(Schema::from(cols))
    }
}