use itertools::izip;

use crate::forecast::TimeType;
use crate::forecast::TimeValue;
use crate::forecast::ValueType;
use crate::forecast::cell::CellEstimator;

#[derive(Default)]
pub struct RowForecast {
  cell_estimators: Vec<Box<dyn CellEstimator>>,
}

impl RowForecast {

  pub fn push_estimator(&mut self, estimator: Box<dyn CellEstimator>) {
    self.cell_estimators.push(estimator);
  }

  pub fn fit_transform(&mut self, values: Vec<ValueType>, time: TimeType, final_time: TimeType) -> Vec<ValueType> {
    assert_eq!(values.len(), self.cell_estimators.len());
    izip!(&mut self.cell_estimators, values)
      .map(|(est, v)| {
        est.consume(&TimeValue { t: time, v });
        let f = est.produce();
        f.predict(final_time as f64)
      })
      .collect()
  }
}