use std::{cell::RefCell, marker::PhantomData};

use getset::{Setters, Getters};
use polars::prelude::*;

use crate::{processor::{MessageProcessor}, graph::ExecutionNode};


/// Factory class for creating an ExecutionNode that can perform AccumulatorOp.
/// 
/// See [AccumulatorBase] to understand the role of individual fields.
#[derive(Getters, Setters)]
pub struct AccumulatorNode<T, P: AccumulatorOp<T>> {
    #[set = "pub"]
    accumulator: P,

    // Necessary to have T as a generic type
    phantom: PhantomData<T>,
}

/// Creates an identity accumulator that passes all the information as it is.
impl<T, P: AccumulatorOp<T> + Clone> Default for AccumulatorNode<T, P> {
    fn default() -> Self {
        Self { 
            accumulator: P::new(),
            phantom: PhantomData::default()
        }
    }
}

impl<T: 'static + Send, P> AccumulatorNode<T, P> 
where
    P: 'static + AccumulatorOp<T> + MessageProcessor<T> + Clone
{
    pub fn new() -> Self {
        AccumulatorNode::default()
    }

    pub fn accumulator(&mut self, op: P) -> &mut Self {
        self.accumulator = op;
        self
    }

    pub fn build(&self) -> ExecutionNode<T> {
        let data_processor = Box::new(self.accumulator.clone());
        ExecutionNode::<T>::new(data_processor, 1)
    }
}

/// The internal accumulation operation performed by [AccumulatorNode].
/// 
/// This operation type supposed to accumulate the results that have been observed thus far
/// in some way (where the way should be defined by implementing structs).
pub trait AccumulatorOp<T> : Send {
    /// Creates a new struct with an initial state.
    fn new() -> Self;

    /// Given a new dataframe, accumulates it with the other dataframes that have been observed
    /// thus far, and produces a new result. The past observations must be kept in the implementing
    /// struct itself.
    /// 
    /// @df A new dataframe.
    /// 
    /// @return The accumulation result.
    fn accumulate(&self, df: &T) -> T;
}

/// Accumulates the result of aggregation based on grouping keys. A common use case is to 
/// compute the up-to-date aggregate results from a series of data.
/// 
/// This is an important example struct that implements [AccumulatorOp]. In the future, different
/// types of accumulators may be added.
#[derive(Getters, Setters, Clone)]
pub struct SumAccumulator {
    #[set = "pub"]
    #[get = "pub"]
    group_key: Vec<String>,

    /// Used to store accumulation thus far
    #[set = "pub"]
    #[get = "pub"]
    accumulated: RefCell<DataFrame>,
}

/// Needed to be sent to different threads.
unsafe impl Send for SumAccumulator {}

impl SumAccumulator {
    pub fn new() -> Self {
        SumAccumulator { group_key: vec![], accumulated: RefCell::new(DataFrame::empty()) }
    }

    /// Aggregates a single dataframe itself without considering the past observations. Used
    /// inside [SumAccumulator::accumulate].
    fn aggregate(&self, df: &DataFrame) -> DataFrame {
        if self.group_key.is_empty() {
            df.sum()
        } else {
            let grouped_df = df.groupby(&self.group_key).unwrap();
            let mut df_agg = grouped_df.sum().unwrap();
            df_agg.set_column_names(
                &self.restore_column_names(&df_agg)
            ).unwrap();
            df_agg
        }
    }

    fn restore_column_names(&self, df: &DataFrame) -> Vec<String> {
        df.get_column_names().into_iter()
            .map(|n| {
                match n.strip_suffix("_sum") {
                    Some(a) => a,
                    None => n,
                }.to_string()
            })
            .collect()
    }
}

impl AccumulatorOp<DataFrame> for SumAccumulator {
    fn accumulate(&self, df: &DataFrame) -> DataFrame {
        let df_agg = self.aggregate(df);
        
        // accumulate
        let df_acc_new;
        {
            let df_acc = &*self.accumulated.borrow();
            let stacked = df_acc.vstack(&df_agg).unwrap();
            df_acc_new = self.aggregate(&stacked);
        }
        
        // save
        *self.accumulated.borrow_mut() = df_acc_new.clone();

        df_acc_new        
    }

    fn new() -> Self {
        SumAccumulator::new()
    }
}

impl MessageProcessor<DataFrame> for SumAccumulator {
    fn process_msg(&self, input: &DataFrame) -> Option<DataFrame> {
        Some(self.accumulate(input))
    }
}


#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use crate::{polars_operations::util::tests::truncate_df, data::DataMessage, graph::NodeReader};

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
        let s0 = DateChunked::parse_from_str_slice("date", dates, DATE_FMT)
                .into_series();
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
        let sum_node = 
            AccumulatorNode::<DataFrame, SumAccumulator>::new().build();
        let input_df = get_example_df().select(["temp", "rain"]).unwrap();
        sum_node.write_to_self(0, DataMessage::from(input_df));
        sum_node.write_to_self(0, DataMessage::eof());
        let reader_node = NodeReader::new(&sum_node);
        sum_node.run();

        let expected_df = df![
            "temp" => [47],
            "rain" => [0.71],
        ].unwrap();
        loop {
            let message = reader_node.read();
            if message.is_eof() {
                break;
            }
            let mut output_df = message.datablock().data().clone();
            truncate_df(&mut output_df, "rain", 3);
            assert_eq!(output_df, expected_df);
        }
    }

    #[test]
    fn sum_accumulator_on_empty() {
        // Test sum with no groups
        let sum_acc = SumAccumulator::new();
        let df = get_example_df().select(["temp", "rain"]).unwrap();
        sum_acc.accumulate(&df);
        let mut acc_df = sum_acc.accumulated().borrow_mut();
        truncate_df(&mut acc_df, "rain", 3);
        
        let expected_df = df![
            "temp" => [47],
            "rain" => [0.71],
        ].unwrap();
        assert_eq!(&*acc_df, &expected_df);

        // Test sum with groups
        let mut sum_acc = SumAccumulator::new();
        sum_acc.set_group_key(vec!["date".into()]);
        let df = get_example_df();
        sum_acc.accumulate(&df);
        let mut acc_df = 
            sum_acc.accumulated().borrow_mut()
                .sort(["date"], false).unwrap();
        truncate_df(&mut acc_df, "rain", 3);
        let expected_df = df![
                "date" => DateChunked::parse_from_str_slice("date", 
                    &[
                        "2020-08-21",
                        "2020-08-22",
                        "2020-08-23",
                    ], DATE_FMT
                ).into_series(),
                "temp" => [30, 8, 9],
                "rain" => [0.3, 0.31, 0.1],
            ].unwrap()
            .sort(["date"], false).unwrap();
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

        let distinct_dates = &[
            "2020-08-21",
            "2020-08-22",
            "2020-08-23",
        ];

        // Count per group
        let df_count = 
            df.groupby(["date"]).unwrap()
                .count().unwrap()
                .sort(["date"], false).unwrap();
        println!("{:?}", df_count);
        assert_eq!(df_count,
            df![
                "date" => DateChunked::parse_from_str_slice("date", distinct_dates, DATE_FMT)
                    .into_series(),
                "temp_count" => [2, 2, 1] as [u32; 3],
                "rain_count" => [2, 2, 1] as [u32; 3],
            ].unwrap());
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
        let mut df_sum =
            df.groupby(["date"]).unwrap()
                .sum().unwrap()
                .sort(["date"], false).unwrap();
        // truncate floating point numbers for approximate equality check
        truncate_df(&mut df_sum, "rain_sum", 3);
        println!("{:?}", df_sum);
        assert_eq!(df_sum, 
            df![
                "date" => DateChunked::parse_from_str_slice("date", distinct_dates, DATE_FMT)
                    .into_series(),
                "temp_sum" => [30, 8, 9] as [i32; 3],
                "rain_sum" => [0.3, 0.31, 0.1] as [f64; 3],
            ].unwrap());
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
        let df_last =
            df.groupby(["date"]).unwrap()
                .last().unwrap()
                .sort(["date"], false).unwrap();
        println!("{:?}", df_last);
        assert_eq!(df_last,
            df![
                "date" => DateChunked::parse_from_str_slice("date", distinct_dates, DATE_FMT)
                    .into_series(),
                "temp_last" => [10, 1, 9],
                "rain_last" => [0.1, 0.01, 0.1],
            ].unwrap());
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
