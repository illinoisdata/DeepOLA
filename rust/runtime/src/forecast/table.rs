use generator::Generator;
use generator::Gn;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::data::ArrayRow;
use crate::data::DataBlock;
use crate::data::DataCell;
use crate::data::SCHEMA_META_NAME;
use crate::forecast::cell::ForecastSelector;
use crate::forecast::row::RowForecast;
use crate::forecast::TimeType;
use crate::forecast::ValueType;
use crate::graph::ExecutionNode;
use crate::processor::SetProcessorV1;


/* Execution node */

pub struct ForecastNode;

impl ForecastNode {
    pub fn node(
        key_columns: Vec<String>,
        values_columns: Vec<String>,
        final_time: TimeType,
    ) -> ExecutionNode<ArrayRow> {
        let data_processor = TableForecast::new_boxed(key_columns, values_columns, final_time);
        ExecutionNode::<ArrayRow>::from_set_processor(data_processor)
    }
}


/* Table forecast mapper as set processor */

pub type ForecastKey = u64;  // TODO: what's appropriate type?

pub struct TableForecast {
    /*
     * There are 3 column groups
     *   - Key: columns to group values together [TODO: what if keys are not unique]
     *   - Values: columns to be forecasted (numeric only)
     *   - Other: columns that TableForecast ignores
     */
    key_columns: Vec<String>,
    values_columns: Vec<String>,
    final_time: TimeType,

    // State of the forecast estimator per row
    row_states: RefCell<HashMap<ForecastKey, RowForecast>>,
    time_counter: RefCell<TimeType>,  // TODO: feed this from data block
}

impl TableForecast {
    pub fn new(
        key_columns: Vec<String>,
        values_columns: Vec<String>,
        final_time: TimeType,
    ) -> TableForecast {
        TableForecast {
            key_columns,
            values_columns,
            final_time,
            row_states: RefCell::new(HashMap::new()),
            time_counter: RefCell::new(0.0),
        }
    }

    pub fn new_boxed(
        key_columns: Vec<String>,
        values_columns: Vec<String>,
        final_time: TimeType,
    ) -> Box<dyn SetProcessorV1<ArrayRow>> {
        Box::new(Self::new(key_columns, values_columns, final_time))
    }

    fn extract_key(&self, key_cells: Vec<DataCell>) -> ForecastKey {
        assert_eq!(key_cells.len(), self.key_columns.len());  // TODO: unecessary?
        DataCell::vector_hash(key_cells)  // TODO: is this unique?
    }

    fn extract_value(&self, value_cells: Vec<DataCell>) -> Vec<ValueType> {
        assert_eq!(value_cells.len(), self.values_columns.len());  // TODO: unecessary?
        value_cells.iter().map(ValueType::from).collect()
    }

    fn fit_transform(&self, key: ForecastKey, values: Vec<ValueType>) -> Vec<ValueType> {
        match self.row_states.borrow_mut().entry(key) {
              Entry::Occupied(mut entry) => entry.get_mut()
                .fit_transform(values, *self.time_counter.borrow(), self.final_time),
              Entry::Vacant(entry) => entry.insert(self.make_row_forecast())
                .fit_transform(values, *self.time_counter.borrow(), self.final_time),
        }
    }

    fn make_row_forecast(&self) -> RowForecast {
      // TODO: should this be configurable?
      let mut row_forecast = RowForecast::default();
      for _forecast_column in &self.values_columns {
        row_forecast.push_estimator(ForecastSelector::make_with_default_candidates());
      }
      row_forecast
    }
}

impl SetProcessorV1<ArrayRow> for TableForecast {
    fn process_v1<'a>(
        &'a self,
        input_set: &'a DataBlock<ArrayRow>,
    ) -> Generator<'a, (), DataBlock<ArrayRow>> {
        Gn::new_scoped(move |mut s| {
            // HACK: step time forward
            {
                // TODO: feed this from data block
                let mut t = self.time_counter.borrow_mut();
                *t += 1.0;
            }

            // Build output schema metadata
            let input_schema = input_set
                .metadata()
                .get(SCHEMA_META_NAME)
                .unwrap()
                .to_schema();
            let key_indexes: Vec<usize> = self.key_columns
                .iter()
                .map(|column| input_schema.index(column))
                .collect();
            let value_indexes: Vec<usize> = self.values_columns
                .iter()
                .map(|column| input_schema.index(column))
                .collect();

            // run through and forecast each target cell
            let output_records = input_set.data().iter()
                .map(|record| {
                    // extract keys (to select row state) and values (to train and predict)
                    let record_key = self.extract_key(record.slice_indices(&key_indexes));
                    let record_values = self.extract_value(
                        record.slice_indices(&value_indexes));

                    // train and predict on corresponding row state
                    let forecast_values = self.fit_transform(record_key, record_values);

                    // copy forecast values
                    let mut new_record = record.clone();
                    for (forecast_idx, value_idx) in value_indexes.iter().enumerate() {
                        new_record[*value_idx] = DataCell::from(forecast_values[forecast_idx]);
                    }
                    new_record
                })
                .collect();

            // compose message and yield
            let message = DataBlock::new(output_records, input_set.metadata().clone());
            s.yield_(message);
            done!();
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::data::Column;
    use crate::data::DataMessage;
    use crate::data::DataType;
    use crate::data::MetaCell;
    use crate::graph::NodeReader;


    // generate message according to time scale t
    fn example_arrow_message(t: TimeType) -> DataMessage<ArrayRow> {
        let metadata = MetaCell::from(vec![
            Column::from_field("country".into(), DataType::Text),
            Column::from_field("state".into(), DataType::Text),
            Column::from_field("city".into(), DataType::Text),
            Column::from_field("population".into(), DataType::Integer),
            Column::from_field("area".into(), DataType::Float),
        ])
        .into_meta_map();

        let input_rows = vec![
            ArrayRow::from([
                "US".into(),
                "IL".into(),
                "Champaign".into(),
                // ((t * 100.0) as i32).into(),  // TODO: how to keep cell type
                (t * 100.0).into(),
                (t * 1.5f64).into(),
            ]),
            ArrayRow::from([
                "US".into(),
                "IL".into(),
                "Urbana".into(),
                // ((t * 200.0) as i32).into(),  // TODO: how to keep cell type
                (t * 200.0).into(),
                (t * 0.0f64).into(),
            ]),
            ArrayRow::from([
                "US".into(),
                "CA".into(),
                "San Francisco".into(),
                // ((t * 300.0) as i32).into(),  // TODO: how to keep cell type
                (t * 300.0).into(),
                (2.5f64.into()),
            ]),
            ArrayRow::from([
                "US".into(),
                "CA".into(),
                "San Jose".into(),
                // ((t * 400.0) as i32).into(),  // TODO: how to keep cell type
                (t * 400.0).into(),
                (3.5f64.into()),
            ]),
            ArrayRow::from([
                "IN".into(),
                "UP".into(),
                "Lucknow".into(),
                // ((t * 500.0) as i32).into(),  // TODO: how to keep cell type
                (t * 500.0).into(),
                (4.5f64.into()),
            ]),
            ArrayRow::from([
                "IN".into(),
                "UP".into(),
                "Noida".into(),
                // ((t * 600.0) as i32).into(),  // TODO: how to keep cell type
                (t * 600.0).into(),
                (t * 0.0f64).into(),
            ]),
        ];
        DataMessage::from(DataBlock::new(input_rows, metadata))
    }

    // generate smaller message according to time scale t
    fn example_arrow_message_small(t: TimeType) -> DataMessage<ArrayRow> {
        let metadata = MetaCell::from(vec![
            Column::from_field("country".into(), DataType::Text),
            Column::from_field("state".into(), DataType::Text),
            Column::from_field("city".into(), DataType::Text),
            Column::from_field("population".into(), DataType::Integer),
            Column::from_field("area".into(), DataType::Float),
        ])
        .into_meta_map();

        let input_rows = vec![
            ArrayRow::from([
                "US".into(),
                "IL".into(),
                "Champaign".into(),
                // ((t * 100.0) as i32).into(),  // TODO: how to keep cell type
                (t * 100.0).into(),
                (t * 1.5f64).into(),
            ]),
            ArrayRow::from([
                "US".into(),
                "IL".into(),
                "Urbana".into(),
                // ((t * 200.0) as i32).into(),  // TODO: how to keep cell type
                (t * 200.0).into(),
                (t * 0.0f64).into(),
            ]),
            ArrayRow::from([
                "US".into(),
                "CA".into(),
                "San Francisco".into(),
                // ((t * 300.0) as i32).into(),  // TODO: how to keep cell type
                (t * 300.0).into(),
                (2.5f64.into()),
            ]),
        ];
        DataMessage::from(DataBlock::new(input_rows, metadata))
    }

    fn make_arrow_forecast_node(final_time: TimeType) -> ExecutionNode<ArrayRow> {
        let key_columns = vec![
            "country".to_string(),
            "state".to_string(),
            "city".to_string(),
        ];
        let values_columns = vec![
            "population".to_string(),
            "area".to_string(),
        ];
        ForecastNode::node(key_columns, values_columns, final_time,)
    }

    #[test]
    fn test_perfect_arrow_message() {
        let final_time = 10;
        let forecast_node = make_arrow_forecast_node(final_time as TimeType);

        // push partitions
        for t in 1 .. final_time {
            let message = example_arrow_message(t as TimeType);
            forecast_node.write_to_self(0, message)
        }
        forecast_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&forecast_node);
        forecast_node.run();

        // inspect message
        let fc_message = reader_node.read();
        let fc_data = fc_message.datablock().data();
        assert_eq!(fc_data.len(), 6);
        assert_eq!(fc_data[0].len(), 5);

        // expecting perfect prediction on later rows 
        let expected_message = example_arrow_message(final_time as TimeType);
        let expected_data = expected_message.datablock().data();
        for _t in 2 .. final_time {
            let fc_message = reader_node.read();
            let fc_data = fc_message.datablock().data();
            assert_eq!(fc_data, expected_data);
        }
    }

    #[test]
    fn test_perfect_arrow_message_new_value() {
        // suddenly rows with new keys show up after mid_time
        let mid_time = 12;
        let final_time = 20;
        let forecast_node = make_arrow_forecast_node(final_time as TimeType);

        // push partitions
        for t in 1 .. mid_time {
            let message = example_arrow_message_small(t as TimeType);
            forecast_node.write_to_self(0, message)
        }
        for t in mid_time .. final_time {
            let message = example_arrow_message(t as TimeType);
            forecast_node.write_to_self(0, message)
        }
        forecast_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&forecast_node);
        forecast_node.run();

        // inspect message
        let fc_message = reader_node.read();
        let fc_data = fc_message.datablock().data();
        assert_eq!(fc_data.len(), 3);
        assert_eq!(fc_data[0].len(), 5);

        // expecting perfect prediction on later rows 
        let expected_message = example_arrow_message_small(final_time as TimeType);
        let expected_data = expected_message.datablock().data();
        for _t in 2 .. mid_time {
            let fc_message = reader_node.read();
            let fc_data = fc_message.datablock().data();
            assert_eq!(fc_data, expected_data);
        }

        // inspect message, new keys
        let fc_message = reader_node.read();
        let fc_data = fc_message.datablock().data();
        assert_eq!(fc_data.len(), 6);
        assert_eq!(fc_data[0].len(), 5);

        // expecting perfect prediction on later rows 
        let expected_message = example_arrow_message(final_time as TimeType);
        let expected_data = expected_message.datablock().data();
        for _t in mid_time + 1 .. final_time {
            let fc_message = reader_node.read();
            let fc_data = fc_message.datablock().data();
            println!("{:?}\n", expected_data);
            assert_eq!(fc_data, expected_data);
        }
    }
}