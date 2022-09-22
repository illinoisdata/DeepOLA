use pyo3::prelude::*;

pub mod tpch;
pub mod utils;

#[pyfunction]
fn test_function() -> String {
    "Hello World from Rust".to_string()
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn tpch_polars(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(test_function, m)?)?;
    Ok(())
}