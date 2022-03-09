use std::fmt::Debug;

use crate::forecast::score::Score;
use crate::forecast::Series;
use crate::forecast::TimeType;
use crate::forecast::TimeValue;
use crate::forecast::ValueType;


/* Common traits */

/// Forecastor for single-valued time series
pub trait CellForecast: Debug {
    /// Predict value at specific time
    fn predict(&self, time: ValueType) -> ValueType;
    /// Get complexity (e.g. number of parameters) of this forecaster
    fn complexity(&self) -> ValueType;
}

/// Struct that can consume time-value pairs
pub trait CellConsumer: Debug {
    /// Consume next time-value to improve the estimate
    fn consume(&mut self, next_tv: &TimeValue);
}

/// Model estimator that iteratively fit and generates CellForecast
pub trait CellEstimator: CellConsumer {
    /// Produce current best forecaster
    fn produce(&self) -> Box<dyn CellForecast>;
    /// Fit the target model with
    fn fit(&mut self, series: Series) -> Box<dyn CellForecast> {
        series.iter().for_each(|tv| self.consume(&tv));
        self.produce()
    }
}

/// Model estimator that iteratively fit and generates CellForecast
pub trait Averager: CellConsumer {  // mostly to reuse averaging logics
    /// Average of time-series so far
    fn average(&self) -> ValueType;
}

/* More score constructor */

impl Score {

    /// construct score for model with i.i.d. normal distributed error
    pub fn least_square (
        complexity: ValueType,
        est: &dyn CellForecast,
        series: Series
    ) -> Score {
        // residual sum of squares
        let rss: ValueType = series.iter().map(|tv| {
            let v_pred = est.predict(tv.t);
            (tv.v - v_pred).powi(2)
        }).sum();

        // relative log likelihood (ignore model-inpendent terms)
        let n: ValueType = series.len() as ValueType;
        let log_likelihood = - n * (rss / n).ln() / 2.0;  // assume normal error
        Score::new(complexity, log_likelihood, n)
    }
}

/****************************************/
/* Predict constant, regardless of time */

#[derive(Debug)]
pub struct ConstantForecast {
    mean: ValueType,
}

impl ConstantForecast {
    fn new(mean: ValueType) -> ConstantForecast {
        ConstantForecast { mean }
    }
}

impl CellForecast for ConstantForecast {
    fn predict(&self, _time: ValueType) -> ValueType {
        self.mean
    }

    fn complexity(&self) -> ValueType {
        1.0  // mean
    }
}


/* Last value */

#[derive(Default, Debug)]
pub struct TailEstimator {
    last_value: ValueType,
}

impl CellConsumer for TailEstimator {
    fn consume(&mut self, next_tv: &TimeValue) {
        self.last_value = next_tv.v
    }
}

impl CellEstimator for TailEstimator {
    fn produce(&self) -> Box<dyn CellForecast> {
        Box::new(ConstantForecast::new(self.average()))
    }
}

impl Averager for TailEstimator {
    fn average(&self) -> ValueType {
        self.last_value
    }
}


/* Average over all observation */

#[derive(Default, Debug)]
pub struct MeanEstimator {
    sum: ValueType,
    total_weight: ValueType,
}

impl CellConsumer for MeanEstimator {
    fn consume(&mut self, next_tv: &TimeValue) {
        self.sum += next_tv.v;
        self.total_weight += 1.0;
    }
}

impl CellEstimator for MeanEstimator {
    fn produce(&self) -> Box<dyn CellForecast> {
        Box::new(ConstantForecast::new(self.average()))
    }
}

impl Averager for MeanEstimator {
    fn average(&self) -> ValueType {
        self.sum / self.total_weight
    }
}


/* Exponentially weighted average */
// v(T) = (\sum_{i=1}^n alpha^{(T - t_i) / freq} v_i) / (\sum_{i=1}^n alpha^{(T - t_i))
// TODO: fitting base automatically

#[derive(Debug)]
pub struct SimpleExponentSmoothEstimator {
    alpha: ValueType,
    freq: Option<ValueType>,
    last_time: TimeType,
    sum: ValueType,
    total_weight: ValueType,
}

impl SimpleExponentSmoothEstimator {
    /// Fit with known base and select first time as the frequency
    /// High alpha corresponds to relying more on past data points
    pub fn with_base(alpha: ValueType) -> SimpleExponentSmoothEstimator {
        assert!(0.0 < alpha && alpha < 1.0);
        SimpleExponentSmoothEstimator {
            alpha,
            freq: None,
            last_time: 0.0,
            sum: 0.0,
            total_weight: 0.0,
        }
    }
}

impl CellConsumer for SimpleExponentSmoothEstimator {
    fn consume(&mut self, next_tv: &TimeValue) {
        if self.freq.is_none() {
            self.freq = Some(next_tv.t);
        }
        let p_delta = (next_tv.t - self.last_time) / self.freq.unwrap();
        let alpha_delta = self.alpha.powf(p_delta);
        self.last_time = next_tv.t;
        self.sum = self.sum * alpha_delta + next_tv.v;
        self.total_weight = self.total_weight * alpha_delta + 1.0;
    }
}

impl CellEstimator for SimpleExponentSmoothEstimator {
    fn produce(&self) -> Box<dyn CellForecast> {
        Box::new(ConstantForecast::new(self.average()))
    }
}

impl Averager for SimpleExponentSmoothEstimator {
    fn average(&self) -> ValueType {
        self.sum / self.total_weight
    }
}

// TODO: combine Tail, Mean, and SES into one weighted average estimator?

// TODO: Gaussian process library?


/************************/
/* Predict linear model */

#[derive(Debug)]
pub struct AffineForecast {
    slope: ValueType,
    intercept: ValueType
}

impl AffineForecast {
    fn new(slope: ValueType, intercept: ValueType) -> AffineForecast {
        AffineForecast { slope, intercept }
    }
}

impl CellForecast for AffineForecast {
    fn predict(&self, time: ValueType) -> ValueType {
        time * self.slope + self.intercept
    }

    fn complexity(&self) -> ValueType {
        2.0  // slope + intercept
    }
}


/* Least-square affine estimator */
// keep track of statistics to compute covariances and means
// from https://stats.stackexchange.com/questions/23481

#[derive(Default, Debug)]
pub struct LeastSquareAffineEstimator {
    var_t: TimeType,
    cov_tv: ValueType,
    mean_t: TimeType,
    mean_v: ValueType,
    n: ValueType,
}

impl LeastSquareAffineEstimator {
    fn make_affine(&self) -> AffineForecast {
        if self.var_t == 0.0 {
            return AffineForecast::new(0.0, self.mean_v);
        }
        let slope = self.cov_tv / self.var_t;  // (X^T X)^{-1} X^T Y
        let intercept = self.mean_v - slope * self.mean_t;
        AffineForecast::new(slope, intercept)
    }
}

impl CellConsumer for LeastSquareAffineEstimator {
    fn consume(&mut self, next_tv: &TimeValue) {
        self.n += 1.0;
        let dt = next_tv.t - self.mean_t;
        let dv = next_tv.v - self.mean_v;
        let correction = (self.n - 1.0) / self.n;
        // let correction = 1.0;
        self.var_t += (correction * dt * dt - self.var_t) / self.n;
        self.cov_tv += (correction * dt * dv - self.cov_tv) / self.n;
        self.mean_t += dt / self.n;
        self.mean_v += dv / self.n;
    }
}

impl CellEstimator for LeastSquareAffineEstimator {
    fn produce(&self) -> Box<dyn CellForecast> {
        Box::new(self.make_affine())
    }
}


/* Average trend line estimator */
// v(T) = v_last + average_trend * (T - t_last)

#[derive(Debug)]
pub struct AverageTrendAffineEstimator {
    trend_estimator: Box<dyn Averager>,
    last_t: TimeType,
    last_v: ValueType,
}

impl AverageTrendAffineEstimator {
    fn with(trend_estimator: Box<dyn Averager>) -> AverageTrendAffineEstimator {
        AverageTrendAffineEstimator {
            trend_estimator,
            last_t: 0.0,
            last_v: 0.0,
        }
    }

    pub fn with_tail() -> AverageTrendAffineEstimator {
        AverageTrendAffineEstimator::with(Box::new(TailEstimator::default()))
    }

    pub fn with_mean() -> AverageTrendAffineEstimator {
        AverageTrendAffineEstimator::with(Box::new(MeanEstimator::default()))
    }

    pub fn with_ses(alpha: ValueType) -> AverageTrendAffineEstimator {
        AverageTrendAffineEstimator::with(Box::new(
            SimpleExponentSmoothEstimator::with_base(alpha)
        ))
    }

    fn make_affine(&self) -> AffineForecast {
        let slope = self.trend_estimator.average();
        let intercept = self.last_v - slope * self.last_t;
        AffineForecast::new(slope, intercept)
    }
}

impl CellConsumer for AverageTrendAffineEstimator {
    fn consume(&mut self, next_tv: &TimeValue) {
        let dt = next_tv.t - self.last_t;
        let dv = next_tv.v - self.last_v;
        self.trend_estimator.consume(&TimeValue { t: next_tv.t, v: dv / dt });
        self.last_t = next_tv.t;
        self.last_v = next_tv.v;
    }
}

impl CellEstimator for AverageTrendAffineEstimator {
    fn produce(&self) -> Box<dyn CellForecast> {
        Box::new(self.make_affine())
    }
}


// TODO: Richardson's extrapolation on reciprocal coordinate?


/******************/
/* Model selector */

#[derive(Debug)]
struct RollingMAECellEstimator {
    estimator: Box<dyn CellEstimator>,
    rolling_err: MeanEstimator,  // use Box<dyn Averager> instead?
}

impl RollingMAECellEstimator {
    fn new(estimator: Box<dyn CellEstimator>) -> RollingMAECellEstimator {
        RollingMAECellEstimator {
            estimator,
            rolling_err: MeanEstimator::default(),
        }
    }

    fn consume_eval(&mut self, train_tv: &TimeValue, eval_tv: &TimeValue) {
        // train on train_tv, eval on eval_tv
        self.estimator.consume(train_tv);
        let pred_v = self.estimator.produce().predict(eval_tv.t);
        let abs_err = (eval_tv.v - pred_v).abs();
        self.rolling_err.consume(&TimeValue { t: train_tv.t, v: abs_err });
    }

    fn produce_with_mae(&self) -> (Box<dyn CellForecast>, ValueType) {
        (self.estimator.produce(), self.rolling_err.average())
    }
}

#[derive(Default, Debug)]
pub struct ForecastSelector {
    scored_estimators: Vec<RollingMAECellEstimator>,
    hot_sample: Option<TimeValue>,  // use queue to hold multiple hot samples
    num_samples: ValueType,
}

impl ForecastSelector {
    pub fn include(&mut self, estimator: Box<dyn CellEstimator>) {
        self.scored_estimators.push(RollingMAECellEstimator::new(estimator))
    }

    fn default_forecast(&self) -> Box<dyn CellForecast> {
      match &self.hot_sample {
        Some(train_tv) => Box::new(ConstantForecast::new(train_tv.v)),
        None => Box::new(ConstantForecast::new(0.0)),
      }
    }
}

impl CellConsumer for ForecastSelector {
    fn consume(&mut self, next_tv: &TimeValue) {
        // use hot sample to train
        if let Some(train_tv) = &self.hot_sample {
            for est in &mut self.scored_estimators {
                est.consume_eval(train_tv, next_tv);
            }
            self.num_samples += 1.0;
        }

        // update hot sample
        self.hot_sample = Some(next_tv.clone())
    }
}

impl CellEstimator for ForecastSelector {
    fn produce(&self) -> Box<dyn CellForecast> {
        if self.num_samples == 0.0 {
            return self.default_forecast();
        }
        let (best_f, _best_score) = self.scored_estimators.iter().map(|est| {
            let (f, mae) = est.produce_with_mae();
            let score = -mae;
            (f, score)
        })
            .max_by(|(_f_1, score_1), (_f_2, score_2)| score_1.partial_cmp(score_2).unwrap())
            .expect("No estimator installed with this ForecastSelector");
        best_f
    }
}


/* Unit tests */

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn assert_forecast(f1: &Box<dyn CellForecast>, f2: &Box<dyn CellForecast>) {
        let mut rng = rand::thread_rng();
        for _ in 0 .. 100 {
            let t = rng.gen_range(0.0..1000.0);
            assert!(
                (f1.predict(t) - f2.predict(t)).abs() < 1e-4,
                "Actual forecaster: {:?}, expected: {:?}", f1, f2,
            );
        }
    }

    #[test]
    fn test_constant_tail() {
        let times = vec![1.0, 2.0, 3.0, 5.0];
        let values = vec![1.0, 2.0, -6.0, 27.0];
        let series = Series::new(&times, &values);
        let mut est = TailEstimator::default();
        for tv in series.iter() {
            est.consume(&tv);
            assert_eq!(est.produce().predict(10.0), tv.v);
            assert_eq!(est.average(), tv.v);
        }
    }

    #[test]
    fn test_constant_mean() {
        let times = vec![1.0, 2.0, 3.0, 5.0];
        let values = vec![1.0, 2.0, -6.0, 27.0];
        let averages = vec![1.0, 1.5, -1.0, 6.0];
        let series = Series::new(&times, &values);
        let mut est = MeanEstimator::default();
        for (tv, expected_avg) in series.iter().zip(averages.iter()) {
            est.consume(&tv);
            assert_eq!(est.produce().predict(10.0), *expected_avg);
            assert_eq!(est.average(), *expected_avg);
        }
    }

    #[test]
    fn test_constant_ses() {
        let times = vec![1.0, 2.0, 3.0, 5.0, 10000.0];
        let values = vec![1.0, 4.0, -5.75, 15.25, 100.0];
        let averages = vec![1.0, 3.0, -2.0, 10.0, 100.0];
        let series = Series::new(&times, &values);
        let mut est = SimpleExponentSmoothEstimator::with_base(0.5);
        for (tv, expected_avg) in series.iter().zip(averages.iter()) {
            est.consume(&tv);
            assert_eq!(est.produce().predict(10000.0), *expected_avg);
            assert_eq!(est.average(), *expected_avg);
        }
    }

    #[test]
    fn test_affine_ols_perfect() {
        let truth = Box::new(AffineForecast::new(1.0, 0.0)) as Box<dyn CellForecast>;
        let times = vec![1.0, 2.0, 3.0, 5.0, 10.0, 12.0];
        let values: Vec<f64> = times.iter().map(|t| truth.predict(*t)).collect();
        let series = Series::new(&times, &values);
        assert_forecast(&LeastSquareAffineEstimator::default().fit(series), &truth)
    }

    #[test]
    fn test_affine_ols_homogeneous() {
        let times = vec![1.0, 2.0, 2.0, 3.0, 3.0, 5.0];
        let values = vec![1.0, 2.5, 1.5, 2.5, 3.5, 5.0];
        let series = Series::new(&times, &values);
        assert_forecast(
            &LeastSquareAffineEstimator::default().fit(series),
            &(Box::new(AffineForecast::new(1.0, 0.0)) as Box<dyn CellForecast>),
        )
    }

    #[test]
    fn test_affine_ols() {
        let times = vec![1.0, 2.0, 2.0, 3.0, 3.0, 5.0];
        let values = vec![11.0, 12.5, 11.5, 12.5, 13.5, 15.0];
        let series = Series::new(&times, &values);
        assert_forecast(
            &LeastSquareAffineEstimator::default().fit(series),
            &(Box::new(AffineForecast::new(1.0, 10.0)) as Box<dyn CellForecast>),
        )
    }

    #[test]
    fn test_affine_trend_tail() {
        let times = vec![1.0, 2.0, 3.0, 5.0];
        let values = vec![1.0, 2.0, -6.0, 27.0];
        let targets = vec![10.0, 10.0, -62.0, 109.5];  // at t = 10
        let series = Series::new(&times, &values);
        let mut est = AverageTrendAffineEstimator::with_tail();
        for (tv, expected_pred) in series.iter().zip(targets.iter()) {
            est.consume(&tv);
            assert_eq!(est.produce().predict(10.0), *expected_pred);
        }
    }

    #[test]
    fn test_affine_trend_mean() {
        let times = vec![1.0, 2.0, 3.0, 5.0];
        let values = vec![1.0, 1.5, 3.0, 7.0];
        let targets = vec![10.0, 7.5, 10.0, 13.25];  // at t = 10
        let series = Series::new(&times, &values);
        let mut est = AverageTrendAffineEstimator::with_mean();
        for (tv, expected_pred) in series.iter().zip(targets.iter()) {
            est.consume(&tv);
            assert_eq!(est.produce().predict(10.0), *expected_pred);
        }
    }

    #[test]
    fn test_affine_trend_ses() {
        let times = vec![1.0, 2.0, 3.0, 5.0];
        let values = vec![1.0, 1.25, 1.40, 2.0];
        let targets = vec![10.0, 5.25, 3.5, 3.5];  // at t = 10
        let series = Series::new(&times, &values);
        let mut est = AverageTrendAffineEstimator::with_ses(0.5);
        for (tv, expected_pred) in series.iter().zip(targets.iter()) {
            est.consume(&tv);
            let actual_pred = est.produce().predict(10.0);
            assert!(
                (actual_pred - *expected_pred).abs() < 1e-4,
                "{} != {}, at est: {:?}, forecaster: {:?}",
                actual_pred, expected_pred, est, est.produce()
            );
        }
    }

    fn make_test_candidates() -> ForecastSelector {
        let mut selector = ForecastSelector::default();
        selector.include(Box::new(TailEstimator::default()));
        selector.include(Box::new(MeanEstimator::default()));
        selector.include(Box::new(SimpleExponentSmoothEstimator::with_base(0.5)));
        selector.include(Box::new(LeastSquareAffineEstimator::default()));
        selector.include(Box::new(AverageTrendAffineEstimator::with_tail()));
        selector.include(Box::new(AverageTrendAffineEstimator::with_mean()));
        selector.include(Box::new(AverageTrendAffineEstimator::with_ses(0.5)));
        selector
    }

    #[test]
    fn test_selector_affine_perfect() {
        let truth = Box::new(AffineForecast::new(5.0, 10.0)) as Box<dyn CellForecast>;
        let times = vec![1.0, 2.0, 3.0, 5.0, 10.0, 12.0];
        let values: Vec<f64> = times.iter().map(|t| truth.predict(*t)).collect();
        let series = Series::new(&times, &values);
        let mut est = make_test_candidates();
        assert_forecast(&est.fit(series), &truth)
    }

    #[test]
    fn test_selector_convergent_perfect() {
        let times: Vec<TimeType> = (1..102).map(|t| t.into()).collect();
        let values: Vec<ValueType> = times.iter().map(|t| 100.0 - 100.0 / t).collect();
        let series = Series::new(&times, &values);
        let mut est = make_test_candidates();
        let f = est.fit(series);
        let pred_v = f.predict(1100.0);
        assert!((pred_v - 99.0).abs() < 1e-3, "Inaccurate f= {:?}, pred= {}", f, pred_v);
    }

    #[test]
    fn test_selector_ln_perfect() {
        let times: Vec<TimeType> = (1..100).map(|t| t.into()).collect();
        let values: Vec<ValueType> = times.iter().map(|t| t.ln()).collect();
        let series = Series::new(&times, &values);
        let mut est = make_test_candidates();
        let f = est.fit(series);
        let pred_v = f.predict(200.0);
        assert!((pred_v - 200.0f64.ln()).abs() < 1.0, "Inaccurate f= {:?}, pred= {}", f, pred_v);
    }


}
