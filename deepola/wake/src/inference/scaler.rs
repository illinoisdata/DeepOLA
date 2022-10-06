use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::series::Series;
use std::cell::RefCell;
use std::rc::Rc;

use crate::channel::MultiChannelBroadcaster;
use crate::channel::MultiChannelReader;
use crate::data::DEFAULT_GROUP_COLUMN_COUNT;
use crate::forecast::cell::CellConsumer;
use crate::forecast::cell::LeastSquareAffineEstimator;
use crate::forecast::TimeValue;
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

    fn set_power(&mut self, power: f64) {
        self.power = power;
    }

    fn estimate(&self, count: CountType, fraction: f64) -> f64 {
        assert!((0.0..=1.0).contains(&fraction));
        if self.power == 0.0 || fraction == 0.0 {
            count.into()
        } else {
            ((count as f64) / fraction.powf(self.power)).round()
        }
    }
}


/// Scale aggregates
pub struct AggregateScaler {
    /// Aggregates to be scaled
    aggregates: Vec<(String, AggregationType)>,

    /// Cardinality estimator
    count_estimator: RefCell<PowerCardinalityEstimator>,

    /// Column name for group count
    count_col: String,

    /// Whether to remove group count column after scaling
    remove_count_col: bool,

    /// Estimate power to the mean count
    power_estimator: RefCell<LeastSquareAffineEstimator>,
}

unsafe impl Send for AggregateScaler {}

/// Creation methods
impl AggregateScaler {
    fn new(count_estimator: PowerCardinalityEstimator) -> AggregateScaler {
        AggregateScaler {
            aggregates: vec![],
            count_estimator: RefCell::new(count_estimator),
            count_col: DEFAULT_GROUP_COLUMN_COUNT.into(),
            remove_count_col: false,
            power_estimator: RefCell::new(LeastSquareAffineEstimator::default()),
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

/// Method-of-mement estimator (MM0)

type RealFn = Box<dyn Fn(f64) -> f64>;

// Newton-Raphson Method
fn newton_raphson(f: RealFn, dfdx: RealFn, mut x0: f64, max_iter: usize, tol: f64) -> f64 {
    for _ in 0..max_iter {
        let diff = f(x0) / dfdx(x0);
        x0 = x0 - diff;
        if diff.abs() < tol {
            break;
        }
    }
    return x0
}

fn generate_mm0_fn(current_count: f64, sample_size: f64) -> (RealFn, RealFn) {
    let f = move |x: f64| {
        x * (1.0 - (-sample_size / x).exp()) - current_count
    };
    let dfdx = move |x: f64| {
        1.0 - (-sample_size / x).exp() * (sample_size + x) / x
    };
    (Box::new(f), Box::new(dfdx))
}

fn mm0_scale(current_count: f64, sample_size: f64, fraction: f64) -> f64 {
    if current_count == sample_size {
        return current_count
    }
    let (f, dfdx) = generate_mm0_fn(current_count, sample_size);
    let estimated_count = newton_raphson(f, dfdx, current_count, 20, 1e-3);
    f64::min(estimated_count, sample_size / fraction)
}

/// Application methods
impl AggregateScaler {
    fn update_power(&self, count: f64, fraction: f64) {
        let mut count_estimator = self.count_estimator.borrow_mut();
        let mut power_estimator = self.power_estimator.borrow_mut();
        let log_count = count.ln();
        let log_fraction = fraction.ln();
        power_estimator.consume(&TimeValue { t: log_fraction, v: log_count });
        let new_power = power_estimator.slope();
        log::error!("count_mean= {:.2}, fraction= {:.2}: slope= {:.2}, intercept= {:.2}", count, fraction, new_power, power_estimator.intercept());
        count_estimator.set_power(new_power)
    }

    fn update_power_many(&self, counts: &Series, fraction: f64, height: usize) {
        let mut count_estimator = self.count_estimator.borrow_mut();
        let mut power_estimator = self.power_estimator.borrow_mut();
        counts.u32()
            .unwrap_or_else(|_| panic!("Count column {} is not u32", self.count_col))
            .into_iter()
            .for_each(|opt_count| {
                let count = opt_count.unwrap() as f64;
                let log_count = count.ln();
                let log_fraction = fraction.ln();
                power_estimator.consume(&TimeValue { t: log_fraction, v: log_count });
            });
        let new_power = power_estimator.slope();
        log::error!("count_mean= {:.2}, fraction= {:.2}: slope= {:.2}, intercept= {:.2}", counts.sum::<f64>().unwrap() / height as f64, fraction, new_power, power_estimator.intercept());
        count_estimator.set_power(new_power)
    }
}
impl MessageFractionProcessor<DataFrame> for AggregateScaler {
    fn process(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        let mut df = df.clone();
        let x0 = df.column(&self.count_col)
            .unwrap_or_else(|_| panic!("Count column {} not found", self.count_col))
            .clone();
        // let x0_mean = x0.sum::<f64>().unwrap() / df.height() as f64;
        // self.update_power(x0_mean, fraction);
        // self.update_power_many(&x0, fraction, df.height());
        // let estimated_groups = mm0_scale(df.height() as f64, x0.sum::<f64>().unwrap(), fraction);
        // self.count_estimator.borrow_mut().set_power(1.0);
        let xhat: Series = x0
            .u32()
            .unwrap_or_else(|_| panic!("Count column {} is not u32", self.count_col))
            .into_iter()
            .map(|opt_count| opt_count.map(
                |count| self.count_estimator.borrow().estimate(count, fraction))
                // |count| self.count_estimator.borrow().estimate(count, fraction) * estimated_groups / df.height() as f64)
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
