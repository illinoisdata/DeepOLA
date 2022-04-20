use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};
use std::cell::{RefCell};

/// The goal of this operation is to support operations
/// that involve subquery in the predicate. The current implementation
/// waits till EOF on the subquery channel, and then processes the data messages
/// on the left input channel. The kind of operations it can be used for are:
/// - WHERE () == (SQ);
/// - WHERE () != (SQ);
/// - WHERE () >= (SQ);
/// - WHERE () <= (SQ);
/// - WHERE () > (SQ);
/// - WHERE () < (SQ);
/// - WHERE () IN (SQ);
/// - WHERE () NOT IN (SQ);
/// WHERE EXISTS (SQ) and NOT EXISTS (SQ) are different from this since
/// the SQ in that case can involve columns from the tables of the outer query.
pub struct WhereSubQueryNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// perform WHERE filter operations with a subquery as right predicate.
impl WhereSubQueryNode {
    pub fn node(left_col: String, operation: String) -> ExecutionNode<ArrayRow> {
        let data_processor = WhereSubQueryProcessor::new_boxed(left_col, operation);
        let num_input = 2;
        ExecutionNode::<ArrayRow>::from_right_complete_processor(data_processor,num_input)
    }
}

/// A struct to represent the WhereSubQueryProcessor.
pub struct WhereSubQueryProcessor {
    left_col: String,
    operation: String,
    right_result: RefCell<Vec<ArrayRow>>,
}

impl WhereSubQueryProcessor {
    pub fn new(left_col: String, operation: String) -> WhereSubQueryProcessor {
        WhereSubQueryProcessor {
            left_col,
            operation,
            right_result: RefCell::new(vec![])
        }
    }

    pub fn new_boxed(left_col: String, operation: String) -> Box<dyn SetMultiProcessor<ArrayRow>> {
        Box::new(Self::new(left_col, operation))
    }

    pub fn _build_output_schema(left_schema: Schema) -> Schema {
        left_schema
    }
}

impl SetMultiProcessor<ArrayRow> for WhereSubQueryProcessor {
    fn pre_process(&self, input_set: &DataBlock<ArrayRow>) {
        let right_datablock_type = input_set.metadata().get(DATABLOCK_TYPE).unwrap();
        let mut right_result = self.right_result.borrow_mut();
        match String::from(right_datablock_type.clone()).as_str() {
            DATABLOCK_TYPE_DM => {
                *right_result = input_set.data().to_vec();
            }
            DATABLOCK_TYPE_DA => {
                for record in input_set.data().iter() {
                    right_result.push(record.clone());
                }
            },
            _ => panic!("Invalid DataBlock Type for WHERE SubQuery")
        }
    }

    fn process<'a>(
        &'a self,
        input_set: &'a DataBlock<ArrayRow>,
    ) -> Generator<'a, (), DataBlock<ArrayRow>> {
        Gn::new_scoped(move |mut s| {
            let input_schema = input_set.metadata().get(SCHEMA_META_NAME).unwrap().to_schema();
            let metadata = MetaCell::Schema(Self::_build_output_schema(
                input_schema.clone()
            )).into_meta_map();

            let left_col_index = input_schema.index(self.left_col.clone());
            let right_values = self.right_result.borrow();

            let mut output_records = vec![];
            for record in input_set.data().iter() {
                let cell = &record[left_col_index];
                let result = match self.operation.as_str() {
                    "==" => cell == &right_values[0][0],
                    "!=" => cell != &right_values[0][0],
                    ">=" => cell >= &right_values[0][0],
                    "<=" => cell <= &right_values[0][0],
                    ">" => cell > &right_values[0][0],
                    "<" => cell < &right_values[0][0],
                    "IN" => {
                        let mut valid = false;
                        for value in right_values.iter() {
                            if &value[0] == cell {
                                valid = true;
                                break;
                            }
                        }
                        valid
                    },
                    "NOT IN" => {
                        let mut valid = true;
                        for value in right_values.iter() {
                            if &value[0] == cell {
                                valid = false;
                                break;
                            }
                        }
                        valid
                    },
                    _ => panic!("Invalid Operation passed to WHERE SubQuery")
                };
                if result {
                    output_records.push(record.clone());
                }
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
    use std::collections::HashMap;

    fn left_meta() -> HashMap<String, MetaCell> {
        MetaCell::from(vec![
            Column::from_field("col1".into(), DataType::Text),
            Column::from_field("col2".into(), DataType::Text),
        ])
        .into_meta_map()
    }

    fn right_meta() -> HashMap<String, MetaCell> {
        MetaCell::from(vec![
            Column::from_field("col3".into(), DataType::Text),
            Column::from_field("col4".into(), DataType::Text),
        ])
        .into_meta_map()
    }

    #[test]
    fn test_where_subquery_node() {
        let left_block = DataBlock::new(
            vec![
                ArrayRow::from(["left1".into(), "1000".into()]),
                ArrayRow::from(["left2".into(), "1000".into()]),
                ArrayRow::from(["left3".into(), "1001".into()]),
                ArrayRow::from(["left4".into(), "1001".into()]),
                ArrayRow::from(["left5".into(), "1001".into()]),
            ]
            .into(),
            left_meta(),
        );
        let right_block = DataBlock::new(
            vec![
                ArrayRow::from(["1000".into()]),
                ArrayRow::from(["1002".into()]),
            ]
            .into(),
            right_meta(),
        );

        // Write the datablocks to left and right channels
        let where_subquery = WhereSubQueryNode::node("col2".into(), "IN".into());

        // Add block to left channel
        where_subquery.write_to_self(0, DataMessage::from(left_block));
        where_subquery.write_to_self(0, DataMessage::eof());

        // Add block to right channel
        where_subquery.write_to_self(1, DataMessage::from(right_block));
        where_subquery.write_to_self(1, DataMessage::eof());

        let reader_node = NodeReader::new(&where_subquery);
        where_subquery.run();

        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let dblock = message.datablock();
            let message_len = dblock.data().len();
            assert_eq!(message_len, 2);
        }
    }
}