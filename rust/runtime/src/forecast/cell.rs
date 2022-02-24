use crate::forecast::ValueType;
use crate::forecast::score::Score;


/* Common traits */

/// Forecastor for single-valued time series
pub trait CellForecast {
  /// Predict value at specific time
  fn predict(&self, time: ValueType) -> ValueType;
}

/// Model estimator that generates CellForecast
pub trait CellEstimator {
  /// Fit the target model with
  fn fit(&self, time: &[ValueType]) -> (Box<dyn CellForecast>, Score);
}


/* More score constructor */

impl Score {

  /// construct score for model with i.i.d. normal distributed error
  pub fn least_square (
    complexity: ValueType,
    est: &Box<dyn CellForecast>,
    series: &[ValueType]
  ) -> Score {
    // residual sum of squares
    let rss: ValueType = series.iter().enumerate().map(|(x, y)| {
      let y_pred: ValueType = est.predict(x as ValueType).into();
      (y - y_pred).powi(2)
    }).sum();

    // relative log likelihood (ignore model-inpendent terms)
    let n: ValueType = series.len() as ValueType;
    let log_likelihood = - n * (rss / n).ln() / 2.0;
    Score::new(complexity, log_likelihood, n)
  }
}

/****************************************/
/* Predict constant, regardless of time */

pub struct ConstantForecast {
  mean: ValueType,
}

impl ConstantForecast {
  fn new(mean: ValueType) -> ConstantForecast {
    ConstantForecast { mean: mean.clone() }
  }
}

impl CellForecast for ConstantForecast {
  fn predict(&self, _time: ValueType) -> ValueType {
    self.mean
  }
}


/* Last value */

pub struct LastEstimator;

impl CellEstimator for LastEstimator {

  fn fit(&self, series: &[ValueType]) -> (Box<dyn CellForecast>, Score) {
    let n = series.len();
    let est = Box::new(ConstantForecast::new(series[n - 1].clone())) as Box<dyn CellForecast>;
    let complexity = 2.0;  // 1 (last) + 1 (error) parameters
    let score = Score::least_square(complexity, &est, series);
    (est, score)
  }
}


/* Average over all observation */

pub struct MeanEstimator;

impl CellEstimator for MeanEstimator {

  fn fit(&self, series: &[ValueType]) -> (Box<dyn CellForecast>, Score) {
    let n = series.len() as ValueType;
    let sum_value: ValueType = series.iter().sum();
    let est = Box::new(ConstantForecast::new(sum_value / n)) as Box<dyn CellForecast>;
    let complexity = 2.0;  // 1 (mean) + 1 (error) parameters
    let score = Score::least_square(complexity, &est, series);
    (est, score)
  }
}


/* Exponentially weighted average */
// TODO: fitting base automatically

pub struct SimpleExponentSmoothEstimator {
  alpha: ValueType,
}

impl SimpleExponentSmoothEstimator {
  /// fit with known base
  pub fn with_base(alpha: ValueType) -> SimpleExponentSmoothEstimator {
    assert!(0.0 < alpha && alpha < 1.0);
    SimpleExponentSmoothEstimator { alpha }
  }

  pub fn weighted_mean(&self, series: &[ValueType]) -> ValueType {
    let n: i32 = series.len().try_into().unwrap();
    let total_weights = 1.0 - (1.0 - self.alpha).powi(n);
    series.iter().enumerate().map(|(x, y)| {
      let x: i32 = x.try_into().unwrap();
      y * self.alpha * (1.0 - self.alpha).powi(n - 1 - x) / total_weights
    }).sum()
  }
}

impl CellEstimator for SimpleExponentSmoothEstimator {

  fn fit(&self, series: &[ValueType]) -> (Box<dyn CellForecast>, Score) {
    let weighted_mean = self.weighted_mean(series);
    let est = Box::new(ConstantForecast::new(weighted_mean)) as Box<dyn CellForecast>;
    let complexity = 2.0;  // 1 (mean) + 1 (error) parameters
    let score = Score::least_square(complexity, &est, series);
    (est, score)
  }
}


/************************/
/* Predict linear model */

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
}
