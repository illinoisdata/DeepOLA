use crate::graph::ExecutionNode;
use crate::data::DATABLOCK_CARDINALITY;
use crate::data::DataBlock;
use polars::frame::DataFrame;
use polars::series::Series;

use crate::channel::MultiChannelBroadcaster;
use crate::channel::MultiChannelReader;
use crate::data::DataMessage;
use crate::data::Payload;
use crate::processor::StreamProcessor;


/// Primitive types
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
}

unsafe impl Send for AggregateScaler {}

/// Creation methods
impl AggregateScaler {
    fn new(count_estimator: PowerCardinalityEstimator, count_col: String) -> AggregateScaler {
        AggregateScaler {
            aggregates: vec![],
            count_estimator,
            count_col,
        }
    }

    pub fn new_complete(count_col: String) -> AggregateScaler {
        AggregateScaler::new(PowerCardinalityEstimator::constant(), count_col)
    }

    pub fn new_converging(count_col: String, power: f64) -> AggregateScaler {
        AggregateScaler::new(PowerCardinalityEstimator::with_power(power), count_col)
    }

    pub fn new_growing(count_col: String) -> AggregateScaler {
        AggregateScaler::new(PowerCardinalityEstimator::linear(), count_col)
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
}

/// Application methods
impl AggregateScaler {
    pub fn scale(&self, mut df: DataFrame, fraction: f64) -> DataFrame {
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
                    &(aggregate_val / &x0) * &xhat
                }).unwrap_or_else(|_| panic!("Failed to scale count at {}", aggregate_col)),
                AggregationType::Count => df.apply(aggregate_col, |aggregate_val| {
                    log::trace!("{:?} ---> {:?}", &aggregate_val, &xhat);
                    xhat.clone()
                }).unwrap_or_else(|_| panic!("Failed to scale count at {}", aggregate_col)),
            };
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
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            match message.payload() {
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Some(dblock) => {
                    let fraction = f64::from(dblock.metadata()
                        .get(DATABLOCK_CARDINALITY)
                        .expect("AggregateScaler requires cardinality fraction"));
                    let output_df = self.scale(dblock.data().clone(), fraction);
                    let output_dblock = DataBlock::new(output_df, dblock.metadata().clone());
                    let output_message = DataMessage::from(output_dblock);
                    output_stream.write(output_message);
                }
                Payload::Signal(_) => {
                    break;
                }
            }
        }
    }
}
