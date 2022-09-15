use crate::forecast::Numeric;
use crate::forecast::ValueType;


/* Criterion that balances complexity and fitness */

pub struct Score {
  complexity: ValueType,  // number of paramters, degree of freedom
  log_likelihood: ValueType,  // maximum likelihood of the model
  num_samples: ValueType,  // number of samples used to estimate the model
}

impl Score {
  pub fn new(complexity: ValueType, log_likelihood: ValueType, num_samples: ValueType) -> Score {
    Score {
        complexity,
        log_likelihood,
        num_samples,
    }
  }

  pub fn new_into<N: Numeric> (complexity: N, log_likelihood: N, num_samples: N) -> Score {
    Score {
        complexity: complexity.into(),
        log_likelihood: log_likelihood.into(),
        num_samples: num_samples.into(),
    }
  }
}

impl Score {

  /// Akaike information criterion (AIC)
  pub fn aic(&self) -> ValueType {
    // 2 k - 2 ln (L)
    2.0 * self.complexity - 2.0 * self.log_likelihood
  }

  /// AIC corrected (AICc) that adjusts for small sample size
  pub fn aicc(&self) -> ValueType {
    // aic + (2k^2 + 2k) / (n - k - 1)
    self.aic() + (
      2.0 * self.log_likelihood.powi(2) + 2.0 * self.log_likelihood
    ) / (
      self.num_samples - self.log_likelihood - 1.0
    )
  }

  /// Bayesian information criterion (BIC)
  pub fn bic(&self) -> ValueType {
    // k ln(n) - 2 ln (L)
    self.complexity * self.num_samples.ln() - 2.0 * self.log_likelihood
  }
}