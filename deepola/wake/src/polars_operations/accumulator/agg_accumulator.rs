use std::rc::Rc;
use std::cell::RefCell;

use getset::{Getters, Setters};
use polars::prelude::*;

use crate::channel::MultiChannelBroadcaster;
use crate::channel::MultiChannelReader;
use crate::data::DEFAULT_GROUP_COLUMN;
use crate::data::DEFAULT_GROUP_COLUMN_COUNT;
use crate::data::DEFAULT_GROUPBY_KEY;
use crate::processor::MessageFractionProcessor;
use crate::processor::StreamProcessor;
use super::AccumulatorOp;

/// Accumulates the result of aggregation based on grouping keys. A common use case is to
/// compute the up-to-date aggregate results from a series of data. The supported set of aggregates
/// are {min, max, sum, count}.
///
/// This is an important example struct that implements [AccumulatorOp]. In the future, different
/// types of accumulators may be added.
#[derive(Getters, Setters, Clone)]
pub struct AggAccumulator {
    #[set = "pub"]
    #[get = "pub"]
    group_key: Vec<String>,

    /// Group attributes
    #[set = "pub"]
    #[get = "pub"]
    group_attributes: Vec<String>,

    /// Used to specify aggregations for each column
    #[set = "pub"]
    #[get = "pub"]
    aggregates: Vec<(String, Vec<String>)>,

    /// Used to store accumulation thus far
    #[set = "pub"]
    #[get = "pub"]
    accumulated: RefCell<DataFrame>,

    /// Whether to add group count column
    #[set = "pub"]
    #[get = "pub"]
    add_count_column: bool,

    /// Use this scaler before writing to output stream
    scaler: Option<Rc<dyn MessageFractionProcessor<DataFrame>>>,
}

/// Needed to be sent to different threads.
unsafe impl Send for AggAccumulator {}

impl Default for AggAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AggAccumulator {
    pub fn new() -> Self {
        AggAccumulator {
            group_key: vec![],
            group_attributes: vec![],
            accumulated: RefCell::new(DataFrame::empty()),
            aggregates: vec![],
            add_count_column: false,
            scaler: None,
        }
    }

    pub fn set_scaler(&mut self, scaler: Rc<dyn MessageFractionProcessor<DataFrame>>) -> &mut Self {
        self.scaler = Some(scaler);
        self
    }

    /// Aggregates a single dataframe itself without considering the past observations. Used
    /// inside [AggAccumulator::accumulate].
    fn aggregate(&self, df: &DataFrame, accumulator: bool) -> DataFrame {
        if self.aggregates().is_empty() {
            panic!("Require aggregates to be specified");
        }

        // Add a column with value 0 to support both empty and non-empty groupby key with same syntax and column name mechanism.
        let num_of_rows = df.height();
        let mut raw_df = df
            .hstack(&[Series::new(DEFAULT_GROUPBY_KEY, vec![0; num_of_rows])])
            .unwrap()
            .hstack(&[Series::new(DEFAULT_GROUP_COLUMN, vec![0; num_of_rows])])
            .unwrap();

        // Arrow raises an error if the Chunks gets misaligned.
        if raw_df.should_rechunk() {
            raw_df.rechunk();
        }
        let mut group_keys = self.group_key().clone();
        group_keys.push(DEFAULT_GROUPBY_KEY.into());

        // Perform the group-by-agg operation and drop the extra column.
        let aggregates = if accumulator {
            self.get_accumulator_aggregates()
        } else {
            self.get_aggregates()
        };
        let mut df_agg = raw_df
            .groupby(&group_keys)
            .unwrap()
            .agg(&aggregates)
            .unwrap();
        let _ = df_agg.drop_in_place(DEFAULT_GROUPBY_KEY).unwrap();
        if accumulator {
            df_agg.set_column_names(&df.get_column_names()).unwrap();
        }
        df_agg
    }

    fn get_accumulator_aggregates(&self) -> Vec<(String, Vec<String>)> {
        let mut acc_aggs = vec![];
        for agg in self.aggregates() {
            for agg_op in &agg.1 {
                let modified_agg_op = if agg_op.as_str() == "count" {
                    "sum" // To join count from two aggregates, need to perform sum.
                } else {
                    agg_op.as_str()
                };
                acc_aggs.push((
                    format!("{}_{}", agg.0, agg_op),
                    vec![format!("{}", modified_agg_op)],
                ));
            }
        }
        if self.add_count_column {
            acc_aggs.push((
                DEFAULT_GROUP_COLUMN_COUNT.into(),
                vec!["sum".into()],  // To join counts, meed to perform sum
            ))
        }
        for attribute in self.group_attributes() {
            acc_aggs.push((
                format!("{}_{}", attribute, "first"),
                vec!["first".into()],
            ))
        }
        acc_aggs
    }

    fn get_aggregates(&self) -> Vec<(String, Vec<String>)> {
        let mut aggregates = self.aggregates().clone();
        if self.add_count_column {
            aggregates.push((
                DEFAULT_GROUP_COLUMN.into(),
                vec!["count".into()],
            ))
        }
        for attribute in self.group_attributes() {
            aggregates.push((
                attribute.into(),
                vec!["first".into()],
            ))
        }
        aggregates
    }
}

impl AccumulatorOp<DataFrame> for AggAccumulator {
    fn accumulate(&self, df: &DataFrame) -> DataFrame {
        let df_agg = self.aggregate(df, false);

        // accumulate
        let df_acc_new;
        {
            let df_acc = &*self.accumulated.borrow();
            let stacked = df_acc.vstack(&df_agg).unwrap();
            df_acc_new = self.aggregate(&stacked, true);
        }

        // save
        *self.accumulated.borrow_mut() = df_acc_new.clone();

        df_acc_new
    }

    fn new() -> Self {
        AggAccumulator::new()
    }
}

/// Note: [MessageProcessor] cannot be implemented for a generic type [AccumulatorOp] because
/// if so, there are multiple generic types implementing [MessageProcessor], which is not allowed
/// in Rust.
impl MessageFractionProcessor<DataFrame> for AggAccumulator {
    fn process(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        let mut accumulated = self.accumulate(&df);
        if let Some(scaler) = &self.scaler {
            accumulated = scaler.process(&accumulated, fraction)
        }
        accumulated
    }
}

impl StreamProcessor<DataFrame> for AggAccumulator {
    fn process_stream(
        &self,
        input_stream: MultiChannelReader<DataFrame>,
        output_stream: MultiChannelBroadcaster<DataFrame>,
    ) {
        self.process_stream_inner(input_stream, output_stream)
    }
}


#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use crate::{
        data::DataBlock,
        data::DataMessage,
        data::MetaCell,
        graph::NodeReader,
        polars_operations::accumulator::AccumulatorNode,
        polars_operations::util::tests::truncate_df,
    };

    use super::*;

    // date format
    static DATE_FMT: &str = "%Y-%m-%d";

    fn get_example_df() -> DataFrame {
        let dates = &[
            "2020-08-21",
            "2020-08-21",
            "2020-08-22",
            "2020-08-23",
            "2020-08-22",
        ];
        // create date series
        let s0 = DateChunked::parse_from_str_slice("date", dates, DATE_FMT).into_series();
        // create temperature series
        let s1 = Series::new("temp", [20, 10, 7, 9, 1]);
        // create rain series
        let s2 = Series::new("rain", [0.2, 0.1, 0.3, 0.1, 0.01]);
        // create a new DataFrame
        let df = DataFrame::new(vec![s0, s1, s2]).unwrap();
        df
    }

    #[test]
    fn sum_accumulator_node() {
        let mut sum_acc = AggAccumulator::new();
        sum_acc.set_aggregates(vec![
            ("temp".into(), vec!["sum".into()]),
            ("rain".into(), vec!["sum".into()]),
        ]);

        let sum_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
            .accumulator(sum_acc)
            .build();
        let input_df = get_example_df().select(["temp", "rain"]).unwrap();
        let input_dblock = DataBlock::new(input_df, MetaCell::from("meta").into_meta_map());
        sum_node.write_to_self(0, DataMessage::from(input_dblock));
        sum_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&sum_node);
        sum_node.run();

        let expected_df = df![
            "temp_sum" => [47],
            "rain_sum" => [0.71],
        ]
        .unwrap();
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let mut output_df = message.datablock().data().clone();
            truncate_df(&mut output_df, "rain_sum", 3);
            assert_eq!(output_df, expected_df);
        }
    }

    #[test]
    fn sum_accumulator_on_empty() {
        // Test sum with no groups
        let mut sum_acc = AggAccumulator::new();
        sum_acc.set_aggregates(vec![
            ("temp".into(), vec!["sum".into()]),
            ("rain".into(), vec!["sum".into()]),
        ]);

        let df = get_example_df().select(["temp", "rain"]).unwrap();
        sum_acc.accumulate(&df);
        let mut acc_df = sum_acc.accumulated().borrow_mut();
        truncate_df(&mut acc_df, "rain_sum", 3);

        let expected_df = df![
            "temp_sum" => [47],
            "rain_sum" => [0.71],
        ]
        .unwrap();
        assert_eq!(&*acc_df, &expected_df);

        // Test sum with groups
        let mut sum_acc = AggAccumulator::new();
        sum_acc
            .set_group_key(vec!["date".into()])
            .set_aggregates(vec![
                ("temp".into(), vec!["sum".into()]),
                ("rain".into(), vec!["sum".into()]),
            ]);

        let df = get_example_df();
        sum_acc.accumulate(&df);
        let mut acc_df = sum_acc
            .accumulated()
            .borrow_mut()
            .sort(["date"], false)
            .unwrap();
        truncate_df(&mut acc_df, "rain_sum", 3);
        let expected_df = df![
            "date" => DateChunked::parse_from_str_slice("date",
                &[
                    "2020-08-21",
                    "2020-08-22",
                    "2020-08-23",
                ], DATE_FMT
            ).into_series(),
            "temp_sum" => [30, 8, 9],
            "rain_sum" => [0.3, 0.31, 0.1],
        ]
        .unwrap()
        .sort(["date"], false)
        .unwrap();
        assert_eq!(&acc_df, &expected_df);
    }

    #[test]
    fn polars_groupby_example() {
        let df = get_example_df();
        println!("{:?}", df);
        // shape: (5, 3)
        // ┌────────────┬──────┬──────┐
        // │ date       ┆ temp ┆ rain │
        // │ ---        ┆ ---  ┆ ---  │
        // │ date       ┆ i32  ┆ f64  │
        // ╞════════════╪══════╪══════╡
        // │ 2020-08-21 ┆ 20   ┆ 0.2  │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        // │ 2020-08-21 ┆ 10   ┆ 0.1  │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        // │ 2020-08-22 ┆ 7    ┆ 0.3  │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        // │ 2020-08-23 ┆ 9    ┆ 0.1  │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        // │ 2020-08-22 ┆ 1    ┆ 0.01 │
        // └────────────┴──────┴──────┘

        let distinct_dates = &["2020-08-21", "2020-08-22", "2020-08-23"];

        // Count per group
        let df_count = df
            .groupby(["date"])
            .unwrap()
            .count()
            .unwrap()
            .sort(["date"], false)
            .unwrap();
        println!("{:?}", df_count);
        assert_eq!(
            df_count,
            df![
                "date" => DateChunked::parse_from_str_slice("date", distinct_dates, DATE_FMT)
                    .into_series(),
                "temp_count" => [2, 2, 1] as [u32; 3],
                "rain_count" => [2, 2, 1] as [u32; 3],
            ]
            .unwrap()
        );
        // shape: (3, 3)
        // ┌────────────┬────────────┬────────────┐
        // │ date       ┆ temp_count ┆ rain_count │
        // │ ---        ┆ ---        ┆ ---        │
        // │ date       ┆ u32        ┆ u32        │
        // ╞════════════╪════════════╪════════════╡
        // │ 2020-08-21 ┆ 2          ┆ 2          │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        // │ 2020-08-22 ┆ 2          ┆ 2          │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        // │ 2020-08-23 ┆ 1          ┆ 1          │
        // └────────────┴────────────┴────────────┘

        // Sum per group
        let mut df_sum = df
            .groupby(["date"])
            .unwrap()
            .sum()
            .unwrap()
            .sort(["date"], false)
            .unwrap();
        // truncate floating point numbers for approximate equality check
        truncate_df(&mut df_sum, "rain_sum", 3);
        println!("{:?}", df_sum);
        assert_eq!(
            df_sum,
            df![
                "date" => DateChunked::parse_from_str_slice("date", distinct_dates, DATE_FMT)
                    .into_series(),
                "temp_sum" => [30, 8, 9] as [i32; 3],
                "rain_sum" => [0.3, 0.31, 0.1] as [f64; 3],
            ]
            .unwrap()
        );
        // shape: (3, 3)
        // ┌────────────┬──────────┬──────────┐
        // │ date       ┆ temp_sum ┆ rain_sum │
        // │ ---        ┆ ---      ┆ ---      │
        // │ date       ┆ i32      ┆ f64      │
        // ╞════════════╪══════════╪══════════╡
        // │ 2020-08-22 ┆ 8        ┆ 0.31     │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
        // │ 2020-08-21 ┆ 30       ┆ 0.3      │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
        // │ 2020-08-23 ┆ 9        ┆ 0.1      │
        // └────────────┴──────────┴──────────┘

        // Last per group
        let df_last = df
            .groupby(["date"])
            .unwrap()
            .last()
            .unwrap()
            .sort(["date"], false)
            .unwrap();
        println!("{:?}", df_last);
        assert_eq!(
            df_last,
            df![
                "date" => DateChunked::parse_from_str_slice("date", distinct_dates, DATE_FMT)
                    .into_series(),
                "temp_last" => [10, 1, 9],
                "rain_last" => [0.1, 0.01, 0.1],
            ]
            .unwrap()
        );
        // shape: (3, 3)
        // ┌────────────┬───────────┬───────────┐
        // │ date       ┆ temp_last ┆ rain_last │
        // │ ---        ┆ ---       ┆ ---       │
        // │ date       ┆ i32       ┆ f64       │
        // ╞════════════╪═══════════╪═══════════╡
        // │ 2020-08-23 ┆ 9         ┆ 0.1       │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
        // │ 2020-08-22 ┆ 1         ┆ 0.01      │
        // ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
        // │ 2020-08-21 ┆ 10        ┆ 0.1       │
        // └────────────┴───────────┴───────────┘
    }
}
