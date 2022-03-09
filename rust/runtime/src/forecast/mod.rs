use core::ops::Add;
use core::ops::AddAssign;
use core::ops::Div;


/* Types that allow required arithmetic operation */

pub trait Numeric:  // unused
    From<usize>
    + Into<f64>
    + Clone
    + Add
    + AddAssign
    + Div<Output=Self>
    + std::iter::Sum<Self>
    + std::slice::SliceIndex<[Self]> {}


pub type TimeType = f64;
pub type ValueType = f64;

#[derive(Clone, Debug)]
pub struct TimeValue {
    pub t: TimeType,
    pub v: ValueType,
}

pub struct Series<'a> {
    pub times: &'a [TimeType],
    pub values: &'a [ValueType],
}

impl<'a> Series<'a> {
    pub fn new(times: &'a [TimeType], values: &'a [ValueType]) -> Series<'a> {
        assert_eq!(times.len(), values.len());
        Series { times, values }
    }

    pub fn iter(&self) -> SeriesIter {
        SeriesIter::new(self)
    }

    fn len(&self) -> usize {
        self.times.len()
    }
}

pub struct SeriesIter<'a> {
    series: &'a Series<'a>,
    idx: usize,
}

impl<'a> SeriesIter<'a> {
    fn new(series: &'a Series<'a>) -> SeriesIter<'a> {
        SeriesIter { series, idx: 0 }
    }
}

impl<'a> Iterator for SeriesIter<'a> {
    type Item = TimeValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.series.len() {
            let item = TimeValue {
                t: self.series.times[self.idx],
                v: self.series.values[self.idx],
            };
            self.idx += 1;
            Some(item)
        } else {
            None
        }
    }
}

/* Imports */

pub mod cell;
pub mod score;