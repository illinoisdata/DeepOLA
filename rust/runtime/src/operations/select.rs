use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};

pub struct SelectNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// perform SELECT operation.
/// TODO: Support Aliasing for Selected Columns
impl SelectNode {
    pub fn node(cols: Vec<String>) -> ExecutionNode<ArrayRow> {
        let data_processor = SelectMapper::new_boxed(cols);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }
}

pub struct SelectMapper {
    cols: Vec<String>
}

impl SelectMapper {
    // Builds the output schema based on the input schema
    pub fn build_output_schema(&self, input_schema: Schema) -> Schema {
        let mut output_columns = Vec::new();
        for col in &self.cols {
            if col == "*" {
                // Push all the schema columns in the output column
                for schema_col in input_schema.columns.clone() {
                    output_columns.push(schema_col);
                }
            } else {
                output_columns.push(input_schema.get_column(col.to_string()));
            }
        }
        Schema::new("unnamed".to_string(), output_columns)
    }

    pub fn new(cols: Vec<String>) -> SelectMapper {
        SelectMapper {
            cols
        }
    }

    pub fn new_boxed(
        cols: Vec<String>
    ) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(Self::new(cols))
    }
}

impl SetProcessorV1<ArrayRow> for SelectMapper {
    fn process_v1<'a>(
        &'a self,
        input_set: &'a DataBlock<ArrayRow>,
    ) -> Generator<'a, (), DataBlock<ArrayRow>> {
        Gn::new_scoped(move |mut s| {
            // Build output schema metadata
            let input_schema = input_set
                .metadata()
                .get(SCHEMA_META_NAME)
                .unwrap()
                .to_schema();
            let output_schema = self.build_output_schema(input_schema.clone());
            let metadata = MetaCell::Schema(output_schema.clone()).into_meta_map();

            let col_indexes = output_schema.columns.iter().map(|x| input_schema.index(x.name.clone())).collect::<Vec<usize>>();
            let mut output_records = Vec::new();
            for record in input_set.data().iter() {
                let filtered_row = col_indexes
                    .iter()
                    .map(|a| record[*a].clone())
                    .collect::<Vec<DataCell>>();
                output_records.push(ArrayRow::from(filtered_row));
            }

            let message = DataBlock::new(output_records, metadata);
            s.yield_(message);
            done!();
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::utils;

    #[test]
    fn test_select_node() {
        let arrayrow_message = utils::example_city_arrow_message();
        let select_cols = vec!["country".to_string(), "state".to_string(), "population".to_string()];
        let select_node = SelectNode::node(select_cols.clone());
        select_node.write_to_self(0, arrayrow_message.clone());
        select_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&select_node);
        select_node.run();

        let message = reader_node.read();
        let data = message.datablock().data();
        let schema = message.datablock().metadata().get(SCHEMA_META_NAME).unwrap();
        assert_eq!(data.len(), arrayrow_message.datablock().len());
        assert_eq!(schema.to_schema().columns.len(), select_cols.len());
    }


    #[test]
    fn test_select_node_star() {
        let arrayrow_message = utils::example_city_arrow_message();
        let select_cols = vec!["*".to_string()];
        let select_node = SelectNode::node(select_cols);
        select_node.write_to_self(0, arrayrow_message.clone());
        select_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&select_node);
        select_node.run();

        let message = reader_node.read();
        let data = message.datablock().data();
        let schema = message.datablock().metadata().get(SCHEMA_META_NAME).unwrap();
        assert_eq!(data.len(), arrayrow_message.datablock().len());
        assert_eq!(schema.to_schema().columns.len(), 5);
    }

}
