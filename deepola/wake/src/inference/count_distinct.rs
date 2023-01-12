use std::cell::RefCell;
use polars::frame::DataFrame;
use polars::prelude::ChunkAgg;
use polars::series::Series;
use statrs::function::gamma::digamma;
use statrs::function::gamma::ln_gamma;
use std::rc::Rc;

use crate::channel::MultiChannelBroadcaster;
use crate::channel::MultiChannelReader;
use crate::graph::ExecutionNode;
use crate::inference::count::PowerCardinalityEstimator;
use crate::processor::MessageFractionProcessor;
use crate::processor::StreamProcessor;


/// Method-of-mement estimator (MM0)

type RealFn = Box<dyn Fn(f64) -> f64>;

// Newton-Raphson Method
fn newton_raphson(f: RealFn, dfdx: RealFn, mut x0: f64, max_iter: usize, tol: f64) -> f64 {
    for _ in 0..max_iter {
        let diff = f(x0) / dfdx(x0);
        x0 -= diff;
        if diff.abs() < tol {
            break;
        }
    }
    x0
}

// fn generate_mm0_fn(current_count: f64, sample_size: f64) -> (RealFn, RealFn) {
//     let f = move |x: f64| {
//         x * (1.0 - (-sample_size / x).exp()) - current_count
//     };
//     let dfdx = move |x: f64| {
//         1.0 - (-sample_size / x).exp() * (sample_size + x) / x
//     };
//     (Box::new(f), Box::new(dfdx))
// }

// fn mm0_scale(current_count: f64, sample_size: f64, total_size: f64) -> f64 {
//     if current_count == sample_size {
//         return current_count
//     }
//     let (f, dfdx) = generate_mm0_fn(current_count, sample_size);
//     let estimated_count = newton_raphson(f, dfdx, current_count, 20, 1e-3);
//     f64::min(estimated_count, total_size)
// }

fn partial_ln_h(x: f64, n: f64, n_total: f64) -> f64 {
    ln_gamma(n_total - x + 1.0) - ln_gamma(n_total - n - x + 1.0)
}

fn partial_dhdx(x: f64, n: f64, n_total: f64) -> f64 {
    digamma(n_total - x + 1.0) - digamma(n_total - n - x + 1.0)
}

fn generate_mm1_fn(current_count: f64, sample_size: f64, total_size: f64) -> (RealFn, RealFn) {
    let ln_h0 = partial_ln_h(0.0, sample_size, total_size);
    let f = move |x: f64| {
        let gx = (partial_ln_h(total_size / x, sample_size, total_size) - ln_h0).exp();
        x * (1.0 - gx) - current_count
    };
    let dfdx = move |x: f64| {
        let gx = (partial_ln_h(total_size / x, sample_size, total_size) - ln_h0).exp();
        let dgx = gx * partial_dhdx(total_size / x, sample_size, total_size);
        (1.0 - gx) - x * dgx * (total_size / (x * x)) 
    };
    (Box::new(f), Box::new(dfdx))
}

fn mm1_scale(current_count: f64, sample_size: f64, total_size: f64) -> f64 {
    if current_count == sample_size || total_size.floor() == sample_size {
        return current_count
    }
    let (f, dfdx) = generate_mm1_fn(current_count, sample_size, total_size);
    let estimated_count = newton_raphson(f, dfdx, current_count, 20, 1e-3);
    // log::error!("current_count= {}, sample_size= {}, total_size= {}: estimated_count= {}", current_count, sample_size, total_size, estimated_count);
    f64::min(estimated_count, total_size)
}


/// Execution node of MM0 estimator
/// Assume that the input df has a count column at count_col, representing the
/// frequency of the key. The actual key column is not required.
pub struct MM0CountDistinct {
    /// Column to groupby
    groupby: Vec<String>,

    /// Name for the output count distinct column
    output_col: String,

    /// Column name for group count
    count_col: String,

    /// Cardinality estimator
    count_estimator: RefCell<PowerCardinalityEstimator>,
}

unsafe impl Send for MM0CountDistinct {}

/// Creation methods
impl MM0CountDistinct {
    pub fn new(groupby: Vec<String>, output_col: String, count_col: String) -> MM0CountDistinct {
        MM0CountDistinct {
            groupby,
            output_col,
            count_col,
            count_estimator: RefCell::new(PowerCardinalityEstimator::linear()),
        }
    }

    pub fn into_node(self) -> ExecutionNode<DataFrame> {
        ExecutionNode::<DataFrame>::new(Box::new(self), 1)
    }

    pub fn into_rc(self) -> Rc<dyn MessageFractionProcessor<DataFrame>> {
        Rc::new(self)
    }
}

/// Scaling methods
impl MM0CountDistinct {
    fn df_mm0(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        let mut stats_df = df.groupby(&self.groupby)
            .expect("Groupby for count distinct failed")
            .agg(&[(&self.count_col, vec!["count", "sum"])])
            .expect("Aggregation for count distinct and total count failed");
        let count_distinct_column = format!("{}_count", self.count_col);
        let count_total_column = format!("{}_sum", self.count_col);
        let count_total = stats_df.column(&count_total_column)
            .expect("Missing count column")
            .u32()
            .expect("Total count column should be UInt32")
            .clone();
        self.count_estimator.borrow_mut()
            .update_power(count_total.sum().unwrap().into(), stats_df.height() as f64, fraction);
        stats_df.apply(&count_distinct_column, |count_distinct| {
            count_distinct.u32()
                .expect("Count distinct column should be UInt32")
                .into_iter()
                .zip(count_total.into_iter())
                .map(|(cd_opt, ct_opt)| {
                    let cd = cd_opt.unwrap() as f64;
                    let ct = ct_opt.unwrap() as f64;
                    let total_size = self.count_estimator.borrow().estimate(ct, fraction);
                    // mm0_scale(cd, ct, total_size)
                    mm1_scale(cd, ct, total_size)
                })
                .collect::<Series>()
        }).expect("Failed to apply mm0");
        let _ = stats_df.drop_in_place(&count_total_column)
            .expect("Failed to drop total count column");
        let _ = stats_df.rename(&count_distinct_column, &self.output_col).unwrap();
        stats_df
    }
}
impl MessageFractionProcessor<DataFrame> for MM0CountDistinct {
    fn process(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        if fraction == 1.0 {
            // Exact count distinct.
            let mut stats_df = df.groupby(&self.groupby)
                .unwrap()
                .agg(&[(&self.count_col, vec!["count"])])
                .unwrap();
            let _ = stats_df.rename(
                &format!("{}_count", self.count_col),
                &self.output_col
            ).unwrap();
            stats_df
        } else {
            // Use method-of-moment estimator.
            self.df_mm0(df, fraction)
        }
    }
}


/// Horvitz-Thompson estimator
fn ht_scale(nj: f64, n: f64, n_total: f64) -> f64 {
    let nhatj = (nj / n) * n_total;
    if n_total <  n + nhatj {
        1.0
    } else {
        let ln_h = (ln_gamma(n_total - nhatj + 1.0) + ln_gamma(n_total - n + 1.0)) 
            - (ln_gamma(n_total - n - nhatj + 1.0) + ln_gamma(n_total + 1.0));
        // log::error!("n= {}, n_total= {}, nhatj= {}, h= {}", n, n_total, nhatj, ln_h);
        1.0 / (1.0 - ln_h.exp())   
    }
}


/// Execution node of Horvitz-Thompson estimator
/// Assume that the input df has a count column at count_col, representing the
/// frequency of the key. The actual key column is not required.
pub struct HorvitzThompsonCountDistinct {
    /// Column to groupby
    groupby: Vec<String>,

    /// Name for the output count distinct column
    output_col: String,

    /// Column name for group count
    count_col: String,
}

unsafe impl Send for HorvitzThompsonCountDistinct {}

/// Creation methods
impl HorvitzThompsonCountDistinct {
    pub fn new(
        groupby: Vec<String>, output_col: String, count_col: String
    ) -> HorvitzThompsonCountDistinct {
        HorvitzThompsonCountDistinct {
            groupby,
            output_col,
            count_col,
        }
    }

    pub fn into_node(self) -> ExecutionNode<DataFrame> {
        ExecutionNode::<DataFrame>::new(Box::new(self), 1)
    }

    pub fn into_rc(self) -> Rc<dyn MessageFractionProcessor<DataFrame>> {
        Rc::new(self)
    }
}

/// Scaling methods
impl HorvitzThompsonCountDistinct {
    fn df_ht(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        let stats_df = df.groupby(&self.groupby)
            .expect("Groupby for count distinct failed")
            .apply(|mut group_df| {
                // Horvitz-Thompson mapping inner term
                let n: f64 = group_df.column(&self.count_col)
                    .expect("Missing count column")
                    .sum()
                    .expect("Failed to sum count column");
                let n_total = n / fraction;  // TODO: use cardinality estimation
                group_df.apply(&self.count_col, |count_series| {
                    count_series.u32()
                        .expect("Count distinct column should be UInt32")
                        .into_iter()
                        .map(|count_opt| {
                            // Horvitz-Thompson the inner term
                            ht_scale(count_opt.unwrap() as f64, n, n_total)
                        })
                        .collect::<Series>()
                }).expect("Failed to apply ht_scale");

                // Horvitz-Thompson outer summation
                let mut last_df = group_df.groupby(&self.groupby)
                    .expect("Groupby to sum count distinct failed")
                    .select([&self.count_col])
                    .sum()
                    .expect("Sum count distinct failed");

                // Rename count distinct column
                let count_distinct_column = format!("{}_sum", self.count_col);
                let _ = last_df.rename(&count_distinct_column, &self.output_col).unwrap();
                Ok(last_df)
            })
            .expect("Apply HT estimator on group failed");
        // log::error!("HorvitzThompsonCountDistinct at {}, stats_df: {:?}", fraction, stats_df);
        stats_df
    }
}
impl MessageFractionProcessor<DataFrame> for HorvitzThompsonCountDistinct {
    fn process(&self, df: &DataFrame, fraction: f64) -> DataFrame {
        if fraction == 1.0 {
            // Exact count distinct.
            let mut stats_df = df.groupby(&self.groupby)
                .unwrap()
                .agg(&[(&self.count_col, vec!["count"])])
                .unwrap();
            let _ = stats_df.rename(
                &format!("{}_count", self.count_col),
                &self.output_col
            ).unwrap();
            stats_df
        } else {
            // Use method-of-moment estimator.
            self.df_ht(df, fraction)
        }
    }
}

impl StreamProcessor<DataFrame> for MM0CountDistinct {
    fn process_stream(
        &self,
        input_stream: MultiChannelReader<DataFrame>,
        output_stream: MultiChannelBroadcaster<DataFrame>,
    ) {
        self.process_stream_inner(input_stream, output_stream)
    }
}

impl StreamProcessor<DataFrame> for HorvitzThompsonCountDistinct {
    fn process_stream(
        &self,
        input_stream: MultiChannelReader<DataFrame>,
        output_stream: MultiChannelBroadcaster<DataFrame>,
    ) {
        self.process_stream_inner(input_stream, output_stream)
    }
}
