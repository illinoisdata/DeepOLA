use crate::data::*;
use crate::graph::*;
use crate::processor::*;
use polars::prelude::Expr;
use polars::prelude::DataFrame;

pub struct WhereNode;

/// A factory method for creating `ExecutionNode<Series>` that can
/// perform WHERE filter operations.
impl WhereNode {
    pub fn node(predicate: Expr) -> ExecutionNode<Series> {
        let data_processor = WhereMapper::new_boxed(predicate);
        ExecutionNode::<Series>::from_set_processor(data_processor)
    }
}

pub struct WhereMapper {
    predicate: Expr,
}

impl WhereMapper {
    pub fn new(predicate: polars::prelude::Expr) -> WhereMapper {
        WhereMapper { predicate }
    }

    pub fn new_boxed(
        predicate: polars::prelude::Expr
    ) -> Box<dyn SetProcessorV2<Series>> {
        Box::new(Self::new(predicate))
    }
}

impl SetProcessorV2<Series> for WhereMapper {
    fn _build_output_schema(&self, input_schema: &Schema) -> Schema {
        Schema::new(format!("where({})",input_schema.table), input_schema.columns.clone())
    }

    fn process_v1(
        &self,
        input_set: &DataBlock<Series>,
    ) -> DataBlock<Series> {
        // Build output schema metadata
        let metadata = self._build_output_metadata(input_set.metadata());

        let df = DataFrame::new(input_set.data()).unwrap();
        let mask = df.column(predicate_column)
        df.filter(&mask);
        // Evaluate predicate on dataframe?
    }
}

#[cfg(test)]
mod tests {
    use crate::operations::utils;

    use super::*;

    #[test]
    fn test_where_predicate_function() {
        fn predicate_example(record: Series) -> bool {
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
        fn predicate_example(record: &Series) -> bool {
            (record.values[3] >= 300.into() && record.values[0] == DataCell::from("US")) ||
            (record.values[3] >= 600.into() && record.values[0] == DataCell::from("IN"))
        }
        let Series_message = utils::example_city_arrow_message();
        let where_node = WhereNode::node(predicate_example);
        where_node.write_to_self(0, Series_message);
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
