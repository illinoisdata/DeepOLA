use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};
use rayon::prelude::*;

pub struct ExpressionNode;

#[derive(Clone)]
pub struct Expression {
    pub predicate: fn(&ArrayRow) -> DataCell,
    pub alias: String,
    pub dtype: DataType,
}

impl ExpressionNode {
    pub fn node(expressions: Vec<Expression>) -> ExecutionNode<ArrayRow> {
        let data_processor = ExpressionMapper::new_boxed(expressions);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }
}

pub struct ExpressionMapper {
    expressions: Vec<Expression>,
}

impl ExpressionMapper {
    pub fn new(expressions: Vec<Expression>) -> Self {
        ExpressionMapper { expressions }
    }

    pub fn new_boxed(
        expressions: Vec<Expression>
    ) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(Self::new(expressions))
    }
}

impl SetProcessorV1<ArrayRow> for ExpressionMapper {
    // Builds the output schema based on the input schema, aliases and output data type.
    fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
        let mut output_columns = Vec::new();
        for col in input_schema.columns.clone() {
            output_columns.push(col);
        }

        for expression in self.expressions.iter() {
            output_columns.push(Column::from_field(
                expression.alias.clone(),
                expression.dtype.clone(),
            ));
        }
        Schema::new(format!("expression({})",input_schema.table), output_columns)
    }

    fn process_v1<'a>(
        &'a self,
        input_set: &'a DataBlock<ArrayRow>,
    ) -> Generator<'a, (), DataBlock<ArrayRow>> {
        Gn::new_scoped(move |mut s| {
            let metadata = self._build_output_metadata(input_set.metadata());
            let output_records: Vec<ArrayRow> = input_set.data().into_par_iter().map(|record| {
                // Evaluate all the expressions on each record.
                let mut result = record.clone();
                for expression in self.expressions.iter() {
                    result.values.push((expression.predicate)(record));
                }
                result
            }).collect();
            let message = DataBlock::new(output_records, metadata);
            s.yield_(message);
            done!();
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        data::{ArrayRow, DataCell, DataMessage, DataType, SCHEMA_META_NAME},
        graph::NodeReader,
        operations::{utils, Expression, ExpressionNode},
    };

    #[test]
    fn test_expression_node() {
        fn is_big_city(record: &ArrayRow) -> DataCell {
            if record.values[3] >= DataCell::Integer(500) {
                DataCell::from(1)
            } else {
                DataCell::from(0)
            }
        }
        let expressions = vec![
            Expression {
                predicate: is_big_city,
                alias: "big_city".into(),
                dtype: DataType::Integer,
            }
        ];
        let arrayrow_message = utils::example_city_arrow_message();
        let number_input_cols = arrayrow_message
            .datablock()
            .metadata()
            .get(SCHEMA_META_NAME)
            .unwrap()
            .to_schema()
            .columns
            .len();
        let where_node = ExpressionNode::node(expressions.clone());
        where_node.write_to_self(0, arrayrow_message);
        where_node.write_to_self(0, DataMessage::eof());

        let reader_node = NodeReader::new(&where_node);
        where_node.run();
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let number_output_cols = dblock
                .metadata()
                .get(SCHEMA_META_NAME)
                .unwrap()
                .to_schema()
                .columns
                .len();
            assert_eq!(number_output_cols, number_input_cols + expressions.len());
        }
    }
}
