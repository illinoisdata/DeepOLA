
#[cfg(test)]
pub mod tests {
    use polars::prelude::*;

    /// Reduce the precision of a floating point column.
    /// 
    /// @df The dataframe to work on
    /// @column The column name. This column's data type must be floating point.
    /// @precision The precision. 3 means that 1.0011 will be rounded to 1.001.
    pub fn truncate_df(df: &mut DataFrame, column: &str, precision: u32) {
        let factor = 10_f64.powf(precision as f64);
        df.apply(column, |series| {
            series.f64().unwrap()
                .into_iter()
                .map(|array| {
                    array.map(|v| (v * factor).round() / factor )
                })
                .collect::<Float64Chunked>()
                .into_series()
            }).unwrap();
    }
}
