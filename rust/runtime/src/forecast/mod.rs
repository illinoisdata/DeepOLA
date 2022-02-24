use core::ops::Add;
use core::ops::AddAssign;
use core::ops::Div;


/* Types that allow required arithmetic operation */

pub trait Numeric: 
  From<usize>
  + Into<f64>
  + Clone
  + Add
  + AddAssign
  + Div<Output=Self>
  + std::iter::Sum<Self>
  + std::slice::SliceIndex<[Self]> {}


pub type ValueType = f64;


/* Imports */

pub mod cell;
pub mod score;