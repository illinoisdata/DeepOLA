use std::cell::RefCell;

use getset::{Getters, Setters};
use polars::prelude::*;

use super::AccumulatorOp;
use crate::processor::MessageProcessor;

#[derive(Debug, Clone, PartialEq)]
pub enum MergeAccumulatorStrategy {
    VStack,
    KeepLast,
}

/// Merges the various dataframes input to this channel and then eventually output one dataframe
/// depending on the merge strategy. The available merge strategies are {vstack, keep_last}.
///
/// This accumulator is used when you want to block the query execution and output only when all
/// the inputs are consumed.
#[derive(Getters, Setters, Clone)]
pub struct MergeAccumulator {
    #[set = "pub"]
    #[get = "pub"]
    merge_strategy: MergeAccumulatorStrategy,

    /// Used to store input dataframes received so far.
    #[set = "pub"]
    #[get = "pub"]
    accumulated: RefCell<DataFrame>,
}

/// Needed to be sent to different threads.
unsafe impl Send for MergeAccumulator {}

impl Default for MergeAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeAccumulator {
    pub fn new() -> Self {
        MergeAccumulator {
            merge_strategy: MergeAccumulatorStrategy::KeepLast,
            accumulated: RefCell::new(DataFrame::empty()),
        }
    }

    pub fn post_process_result(&self) -> DataFrame {
        let df = &mut *self.accumulated.borrow_mut();
        if df.should_rechunk() {
            df.rechunk();
        }
        df.clone()
    }
}

impl AccumulatorOp<DataFrame> for MergeAccumulator {
    fn accumulate(&self, df: &DataFrame) -> DataFrame {
        let df_acc = match self.merge_strategy {
            MergeAccumulatorStrategy::KeepLast => df.clone(),
            MergeAccumulatorStrategy::VStack => {
                let df_curr = &mut *self.accumulated.borrow_mut();
                df_curr.vstack(df).unwrap()
            }
        };
        *self.accumulated.borrow_mut() = df_acc;
        DataFrame::empty()
    }

    fn new() -> Self {
        MergeAccumulator::new()
    }
}

/// Note: [MessageProcessor] cannot be implemented for a generic type [AccumulatorOp] because
/// if so, there are multiple generic types implementing [MessageProcessor], which is not allowed
/// in Rust.
impl MessageProcessor<DataFrame> for MergeAccumulator {
    fn process_msg(&self, input: &DataFrame) -> Option<DataFrame> {
        self.accumulate(input);
        None
    }

    fn post_process_msg(&self) -> Option<DataFrame> {
        Some(self.post_process_result())
    }
}
