use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use generator::{Generator, Gn};

pub struct WhereNode;

/// A factory method for creating `ExecutionNode<ArrayRow>` that can
/// perform WHERE filter operations.
impl WhereNode {
    pub fn node(predicate: fn(&ArrayRow) -> bool) -> ExecutionNode<ArrayRow> {
        let data_processor = WhereMapper::new_boxed(predicate);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }
}

pub struct WhereMapper {
    predicate: fn(&ArrayRow) -> bool,
}

impl WhereMapper {
    pub fn new(predicate: fn(&ArrayRow) -> bool) -> WhereMapper {
        WhereMapper { predicate }
    }

    pub fn new_boxed(
        predicate: fn(&ArrayRow) -> bool
    ) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(Self::new(predicate))
    }
}

impl SetProcessorV1<ArrayRow> for WhereMapper {
    fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
        Schema::new(format!("where({})",input_schema.table), input_schema.columns.clone())
    }

    fn process_v1<'a>(
        &'a self,
        input_set: &'a DataBlock<ArrayRow>,
    ) -> Generator<'a, (), DataBlock<ArrayRow>> {
        Gn::new_scoped(move |mut s| {
            // Build output schema metadata
            let metadata = self._build_output_metadata(input_set.metadata());

            // Evaluate predicate on each record
            let mut output_records = vec![];
            for record in input_set.data().iter() {
                let result = (self.predicate)(record);
                if result {
                    output_records.push(record.clone())
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
    use crate::operations::utils;

    use super::*;

    #[test]
    fn test_where_predicate_function() {
        fn predicate_example(record: ArrayRow) -> bool {
            record.values[3] >= 300.into() && record.values[0] == DataCell::from("US")
        }
        let records = utils::example_city_arrow_rows();
        let mut record_count = 0;
        for record in records {
            let result = predicate_example(record.clone());
            if result {
                record_count += 1;
            }
        }
        assert_eq!(record_count,2);
    }

    #[test]
    fn test_where_node() {
        // Test predicate with OR of multiple ANDs
        fn predicate_example(record: &ArrayRow) -> bool {
            (record.values[3] >= 300.into() && record.values[0] == DataCell::from("US")) ||
            (record.values[3] >= 600.into() && record.values[0] == DataCell::from("IN"))
        }
        let arrayrow_message = utils::example_city_arrow_message();
        let where_node = WhereNode::node(predicate_example);
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
            let data = dblock.data();
            let message_len = data.len();
            assert_eq!(message_len, 5);
        }
    }
}
