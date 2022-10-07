use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::series::Series;
use std::rc::Rc;

use crate::channel::MultiChannelBroadcaster;
use crate::channel::MultiChannelReader;
use crate::data::DEFAULT_GROUP_COLUMN_COUNT;
use crate::graph::ExecutionNode;
use crate::processor::MessageFractionProcessor;
use crate::processor::StreamProcessor;


/// Primitive types and consts
type CountType = u32;
enum AggregationType {
    Sum,
    Count,
}


/// Estimate cardinality given progress so far
struct PowerCardinalityEstimator {
    power: f64,
}


impl PowerCardinalityEstimator {
    fn constant() -> PowerCardinalityEstimator {
        PowerCardinalityEstimator {
            power: 0.0,
        }
    }

    fn with_power(power: f64) -> PowerCardinalityEstimator {
        assert!((0.0..=1.0).contains(&power));
        PowerCardinalityEstimator {
            power,
        }
    }

    fn linear() -> PowerCardinalityEstimator {
        PowerCardinalityEstimator {
            power: 1.0,
        }
    }

    fn estimate(&self, count: CountType, fraction: f64) -> CountType {
        assert!((0.0..=1.0).contains(&fraction));
        if self.power == 0.0 || fraction == 0.0 {
            count
        } else {
            ((count as f64) / fraction.powf(self.power)).round() as CountType
        }
    }
}


/// Scale aggregates
pub struct AggregateScaler {
    /// Aggregates to be scaled
    aggregates: Vec<(String, AggregationType)>,

    /// Cardinality estimator
    count_estimator: PowerCardinalityEstimator,

    /// Column name for group count
    count_col: String,

    /// Whether to remove group count column after scaling
    remove_count_col: bool,
}

unsafe impl Send for AggregateScaler {}

/// Creation methods
impl AggregateScaler {
    fn new(count_estimator: PowerCardinalityEstimator) -> AggregateScaler {
        AggregateScaler {
            aggregates: vec![],
            count_estimator,
            count_col: DEFAULT_GROUP_COLUMN_COUNT.into(),
            remove_count_col: false,
        }
    }

    pub fn new_complete() -> AggregateScaler {
        AggregateScaler::new(PowerCardinalityEstimator::constant())
    }

    pub fn new_converging(power: f64) -> AggregateScaler {
        AggregateScaler::new(PowerCardinalityEstimator::with_power(power))
    }

    pub fn new_growing() -> AggregateScaler {
        AggregateScaler::new(PowerCardinalityEstimator::linear())
    }

    pub fn count_column(mut self, count_col: String) -> Self {
        self.count_col = count_col;
        self
    }

    pub fn remove_count_column(mut self) -> Self {
        self.remove_count_col = true;
        self
    }

    pub fn scale_sum(mut self, column: String) -> Self {
        self.aggregates.push((column, AggregationType::Sum));
        self
    }

    pub fn scale_count(mut self, column: String) -> Self {
        self.aggregates.push((column, AggregationType::Count));
        self
    }

    pub fn into_node(self) -> ExecutionNode<DataFrame> {
        ExecutionNode::<DataFrame>::new(Box::new(self), 1)
    }

    pub fn into_rc(self) -> Rc<dyn MessageFractionProcessor<DataFrame>> {
        Rc::new(self)
    }
}

/// Application methods
impl MessageFractionProcessor<DataFrame> for AggregateScaler {
    fn process(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        let mut df = df.clone();
        let x0 = df.column(&self.count_col)
            .unwrap_or_else(|_| panic!("Count column {} not found", self.count_col))
            .clone();
        let xhat: Series = x0
            .u32()
            .unwrap_or_else(|_| panic!("Count column {} is not u32", self.count_col))
            .into_iter()
            .map(|opt_count| opt_count.map(
                |count| self.count_estimator.estimate(count, fraction))
            )
            .collect();
        for (aggregate_col, aggregate_type) in &self.aggregates {
            match aggregate_type {
                AggregationType::Sum => df.apply(aggregate_col, |aggregate_val| {
                    log::trace!("{:?} ---> {:?}", &aggregate_val, &(aggregate_val / &x0) * &xhat);
                    &(&aggregate_val.cast(&DataType::Float64).unwrap() / &x0) * &xhat
                }).unwrap_or_else(|_| panic!("Failed to scale count at {}", aggregate_col)),
                AggregationType::Count => df.apply(aggregate_col, |aggregate_val| {
                    log::trace!("{:?} ---> {:?}", &aggregate_val, &xhat);
                    xhat.clone()
                }).unwrap_or_else(|_| panic!("Failed to scale count at {}", aggregate_col)),
            };
        }
        if self.remove_count_col {
            let _ = df.drop_in_place(&self.count_col).unwrap();
        }
        df
    }
}


impl StreamProcessor<DataFrame> for AggregateScaler {
    fn process_stream(
        &self,
        input_stream: MultiChannelReader<DataFrame>,
        output_stream: MultiChannelBroadcaster<DataFrame>,
    ) {
        self.process_stream_inner(input_stream, output_stream)
    }
}
