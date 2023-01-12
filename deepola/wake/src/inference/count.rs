use std::cell::RefCell;

use crate::forecast::cell::CellConsumer;
use crate::forecast::cell::CellEstimator;
use crate::forecast::cell::LeastSquareAffineEstimator;
use crate::forecast::cell::LeastSquareVariance;
use crate::forecast::TimeValue;


/// Estimate cardinality given progress so far
pub struct PowerCardinalityEstimator {
    power: RefCell<f64>,

    /// Estimate power to the mean count
    power_estimator: RefCell<LeastSquareAffineEstimator>,

    /// To compute variance of slope through residual sum of squared (RSS)
    ols_var: RefCell<Option<LeastSquareVariance>>,
 }


impl PowerCardinalityEstimator {
    pub fn constant() -> PowerCardinalityEstimator {
        PowerCardinalityEstimator {
            power: RefCell::new(0.0),
            power_estimator: RefCell::new(LeastSquareAffineEstimator::default()),
            ols_var: RefCell::new(None),
        }
    }

    pub fn with_power(power: f64) -> PowerCardinalityEstimator {
        assert!((0.0..=1.0).contains(&power));
        PowerCardinalityEstimator {
            power: RefCell::new(power),
            power_estimator: RefCell::new(LeastSquareAffineEstimator::default()),
            ols_var: RefCell::new(None),
        }
    }

    pub fn linear() -> PowerCardinalityEstimator {
        PowerCardinalityEstimator {
            power: RefCell::new(1.0),
            power_estimator: RefCell::new(LeastSquareAffineEstimator::default()),
            ols_var: RefCell::new(None),
        }
    }

    pub fn track_variance(&mut self) {
        *self.ols_var.borrow_mut() = Some(LeastSquareVariance::default());
    }

    pub fn estimate(&self, count: f64, fraction: f64) -> f64 {
        assert!((0.0..=1.0).contains(&fraction));
        if *self.power.borrow() == 0.0 || fraction == 0.0 {
            count
        } else {
            (count / fraction.powf(*self.power.borrow())).round()
        }
    }

    pub fn estimate_with_variance(&self, count: f64, fraction: f64) -> (f64, f64) {
        let slope_variance = self.ols_var.borrow().as_ref()
            .expect("Need to enable variance tracking beforehand")
            .slope_variance();
        let estimate_count = self.estimate(count, fraction);
        let count_variance = slope_variance * estimate_count.powi(2) * (1.0 / fraction).ln();
        (estimate_count, count_variance)
    }

    pub fn update_power(&self, total_count: f64, group_count: f64, fraction: f64) {
        let mut power_estimator = self.power_estimator.borrow_mut();
        let count = total_count / group_count;  // Average group cardinality.
        let log_count = count.ln();
        let log_fraction = fraction.ln();
        power_estimator.consume(&TimeValue { t: log_fraction, v: log_count });
        // log::error!("count_mean= {:.2}, fraction= {:.2}: slope= {:.2}, intercept= {:.2}", count, fraction, power_estimator.slope(), power_estimator.intercept());
        self.set_power(power_estimator.slope());

        if let Some(ols_var_borrow) = self.ols_var.borrow_mut().as_mut() {
            let forecast = power_estimator.produce();
            ols_var_borrow.consume(forecast.as_ref(), log_fraction, log_count)
        }
    }

    fn set_power(&self, power: f64) {
        *self.power.borrow_mut() = power;
    }
}