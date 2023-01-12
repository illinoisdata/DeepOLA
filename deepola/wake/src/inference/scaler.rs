use itertools::izip;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::Series;
use std::cell::RefCell;
use std::rc::Rc;

use crate::channel::MultiChannelBroadcaster;
use crate::channel::MultiChannelReader;
use crate::data::DEFAULT_GROUP_COLUMN_COUNT;
use crate::graph::ExecutionNode;
use crate::inference::count::PowerCardinalityEstimator;
use crate::processor::MessageFractionProcessor;
use crate::processor::StreamProcessor;


/// Primitive types and consts
enum AggregationType {
    Sum,
    Count,
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

    /// Whether to propagate variance
    track_variance: bool,
    track_variance_sum_col: Vec<(String, String)>,
    track_covariance_sum_sum_col: Vec<(String, String)>,
    track_variance_count_col: Vec<String>,
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
            track_variance: false,
            track_variance_sum_col: Vec::new(),
            track_covariance_sum_sum_col: Vec::new(),
            track_variance_count_col: Vec::new(),
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

    pub fn scale_sum_with_variance(mut self, column: String, var_column: String) -> Self {
        self.aggregates.push((column.clone(), AggregationType::Sum));
        self.track_variance_sum_col.push((column, var_column));
        self
    }

    pub fn track_sum_sum_covariance(mut self, column_1: String, column_2: String) -> Self {
        self.track_covariance_sum_sum_col.push((column_1, column_2));
        self
    }

    pub fn scale_count(mut self, column: String) -> Self {
        self.aggregates.push((column, AggregationType::Count));
        self
    }

    pub fn scale_count_with_variance(mut self, column: String) -> Self {
        self.aggregates.push((column.clone(), AggregationType::Count));
        self.track_variance_count_col.push(column);
        self
    }

    pub fn track_variance(mut self) -> Self {
        self.count_estimator.borrow_mut().track_variance();
        self.track_variance = true;
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
impl AggregateScaler {
    fn process_with_variance(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        assert!(self.track_variance);
        let mut out_df = df.clone();

        // Update scaling power.
        let x0 = df.column(&self.count_col)
            .unwrap_or_else(|_| panic!("Count column {} not found", self.count_col))
            .clone();
        self.count_estimator.borrow_mut()
            .update_power(x0.sum::<f64>().unwrap(), df.height() as f64, fraction);

        // Estimate final counts
        let (xhat, xhat_var): (Vec<f64>, Vec<f64>) = x0
            .u32()
            .unwrap_or_else(|_| panic!("Count column {} is not u32", self.count_col))
            .into_iter()
            .map(|opt_count| opt_count.map_or((0.0, 0.0), |count| {
                self.count_estimator.borrow().estimate_with_variance(count.into(), fraction)
            }))
            .unzip();
        let xhat: Series = xhat.iter().collect();
        let xhat_var: Series = xhat_var.iter().collect();

        // Scale aggregate accordingly.
        for (aggregate_col, aggregate_type) in &self.aggregates {
            match aggregate_type {
                AggregationType::Sum => out_df.apply(aggregate_col, |aggregate_val| {
                    log::trace!("{:?} ---> {:?}", &aggregate_val, &(aggregate_val / &x0) * &xhat);
                    &(&aggregate_val.cast(&DataType::Float64).unwrap() / &x0) * &xhat
                }).unwrap_or_else(|_| panic!("Failed to scale count at {}", aggregate_col)),
                AggregationType::Count => out_df.apply(aggregate_col, |aggregate_val| {
                    log::trace!("{:?} ---> {:?}", &aggregate_val, &xhat);
                    xhat.clone()
                }).unwrap_or_else(|_| panic!("Failed to scale count at {}", aggregate_col)),
            };
        }

        // Propagate sum variance
        for (sum_col, sample_var_col) in &self.track_variance_sum_col {
            let sum = df.column(sum_col).unwrap();
            let sample_var = df.column(sample_var_col).unwrap();
            let sum_var: Vec<f64> = izip!(
                    x0.u32().unwrap(),
                    xhat.f64().unwrap(),
                    xhat_var.f64().unwrap(),
                    sum.cast(&DataType::Float64).unwrap().f64().unwrap(),
                    sample_var.cast(&DataType::Float64).unwrap().f64().unwrap(),
                ).map(|(x0, xhat, xhat_var, sum, sample_var)| {
                    let x0: f64 = x0.unwrap().into();
                    let xhat: f64 = xhat.unwrap();
                    let xhat_var: f64 = xhat_var.unwrap();
                    let sum: f64 = sum.unwrap();
                    let sample_var: f64 = sample_var.unwrap();
                    let mean_var = sample_var / x0;
                    let sum_var = mean_var * xhat.powi(2) + xhat_var * (sum / x0).powi(2);
                    // println!("x0= {:.0}, xhat= {:.0}, xhat_var= {:.0}, sum= {:.0}, sample_var= {:.0}, mean_var= {:.0}, sum_var= {:.0}", x0, xhat, xhat_var, sum, sample_var, mean_var, sum_var);
                    sum_var
                }).collect();
            out_df.with_column(Series::new(&format!("{}_var", sum_col), sum_var))
                .unwrap_or_else(|_| panic!("Failed to replace variance for sum {}", sum_col));
        }

        // Propagate sum-sum covariance
        for (sum_col_1, sum_col_2) in &self.track_covariance_sum_sum_col {
            let sum_1 = df.column(sum_col_1).unwrap();
            let sum_2 = df.column(sum_col_2).unwrap();
            let sum_sum_cov: Vec<f64> = izip!(
                    x0.u32().unwrap(),
                    xhat_var.f64().unwrap(),
                    sum_1.cast(&DataType::Float64).unwrap().f64().unwrap(),
                    sum_2.cast(&DataType::Float64).unwrap().f64().unwrap(),
                ).map(|(x0, xhat_var, sum_1, sum_2)| {
                    let x0: f64 = x0.unwrap().into();
                    let xhat_var: f64 = xhat_var.unwrap();
                    let sum_1: f64 = sum_1.unwrap();
                    let sum_2: f64 = sum_2.unwrap();
                    xhat_var * (sum_1 / x0) * (sum_2 / x0)
                }).collect();
            out_df.with_column(Series::new(&format!("{}_{}_cov", sum_col_1, sum_col_2), sum_sum_cov))
                .unwrap_or_else(|_| panic!("Failed to replace covariance for sum-sum {}-{}", sum_col_1, sum_col_2));
        }

        // Propagate count variance
        for count_col in &self.track_variance_count_col {
            out_df.with_column(Series::new(&format!("{}_var", count_col), xhat_var.clone()))
                .unwrap_or_else(|_| panic!("Failed to replace variance for count {}", count_col));
        }

        // Remove sample variance columns
        for (_, sample_var_col) in &self.track_variance_sum_col {
            let _ = out_df.drop_in_place(sample_var_col).unwrap();
        }

        if self.remove_count_col {
            let _ = out_df.drop_in_place(&self.count_col).unwrap();
        }
        out_df
    }

    fn process(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        let mut df = df.clone();

        // Update scaling power.
        let x0 = df.column(&self.count_col)
            .unwrap_or_else(|_| panic!("Count column {} not found", self.count_col))
            .clone();
        self.count_estimator.borrow_mut()
            .update_power(x0.sum::<f64>().unwrap(), df.height() as f64, fraction);

        // Estimate final counts
        let xhat: Series = x0
            .u32()
            .unwrap_or_else(|_| panic!("Count column {} is not u32", self.count_col))
            .into_iter()
            .map(|opt_count| opt_count.map(
                |count| self.count_estimator.borrow().estimate(count.into(), fraction))
            )
            .collect();

        // Scale aggregate accordingly.
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

impl MessageFractionProcessor<DataFrame> for AggregateScaler {
    fn process(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        if self.track_variance {
            self.process_with_variance(df, fraction)
        } else {
            self.process(df, fraction)
        }
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

pub fn calculate_div_var(
    a: &Series, 
    a_var: &Series, 
    b: &Series, 
    b_var: &Series, 
    ab_cov: &Series, 
    div: &Series,
) -> Vec<f64> {
    izip!(
        a.f64().unwrap(),
        a_var.f64().unwrap(),
        b.f64().unwrap(),
        b_var.f64().unwrap(),
        ab_cov.f64().unwrap(),
        div.f64().unwrap()
    ).map(|(a, a_var, b, b_var, ab_cov, div)| {
        let a: f64 = a.unwrap();
        let a_var: f64 = a_var.unwrap();
        let b: f64 = b.unwrap();
        let b_var: f64 = b_var.unwrap();
        let div: f64 = div.unwrap();
        let ab_cov: f64 = ab_cov.unwrap();
        div.powi(2) * (a_var / a.powi(2) + b_var / b.powi(2) - 2.0 * ab_cov / (a * b))
    }).collect::<Vec<f64>>()
}
