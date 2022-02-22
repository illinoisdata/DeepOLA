use std::ops::{Add,Sub,Mul,Div};
use super::DataCell;

// Implement Addition
impl Add<&DataCell> for DataCell {
    type Output = DataCell;
    fn add(self, rhs: &DataCell) -> Self::Output {
        match (self,rhs) {
            (DataCell::Integer(a), DataCell::Integer(b)) => DataCell::Integer(a+b),
            (DataCell::Integer(a), DataCell::Float(b)) => DataCell::Float(f64::from(a)+b),
            (DataCell::Float(a), DataCell::Integer(b)) => DataCell::Float(a + f64::from(*b)),
            (DataCell::Float(a), DataCell::Float(b)) => DataCell::Float(a + b),
            _ => panic!("ADD not implemented")
        }
    }
}

impl Add<DataCell> for DataCell {
    type Output = DataCell;
    fn add(self, rhs: DataCell) -> Self::Output {
        self.add(&rhs)
    }
}

// Implement Subtraction
impl Sub<&DataCell> for DataCell {
    type Output = DataCell;
    fn sub(self, rhs: &DataCell) -> Self::Output {
        match (self,rhs) {
            (DataCell::Integer(a), DataCell::Integer(b)) => DataCell::Integer(a-b),
            (DataCell::Integer(a), DataCell::Float(b)) => DataCell::Float(f64::from(a)-b),
            (DataCell::Float(a), DataCell::Integer(b)) => DataCell::Float(a - f64::from(*b)),
            (DataCell::Float(a), DataCell::Float(b)) => DataCell::Float(a - b),
            _ => panic!("SUB not implemented")
        }
    }
}

impl Sub<DataCell> for DataCell {
    type Output = DataCell;
    fn sub(self, rhs: DataCell) -> Self::Output {
        self.sub(&rhs)
    }
}


// Implement Division
impl Div<&DataCell> for DataCell {
    type Output = DataCell;
    fn div(self, rhs: &DataCell) -> Self::Output {
        match (self,rhs) {
            (DataCell::Integer(a), DataCell::Integer(b)) => DataCell::Float(f64::from(a)/f64::from(*b)),
            (DataCell::Integer(a), DataCell::Float(b)) => DataCell::Float(f64::from(a)/b),
            (DataCell::Float(a), DataCell::Integer(b)) => DataCell::Float(a/f64::from(*b)),
            (DataCell::Float(a), DataCell::Float(b)) => DataCell::Float(a/b),
            _ => panic!("DIV not implemented")
        }
    }
}

impl Div<DataCell> for DataCell {
    type Output = DataCell;
    fn div(self, rhs: DataCell) -> Self::Output {
        self.div(&rhs)
    }
}

// Implement Multiplication
impl Mul<&DataCell> for DataCell {
    type Output = DataCell;
    fn mul(self, rhs: &DataCell) -> Self::Output {
        match (self,rhs) {
            (DataCell::Integer(a), DataCell::Integer(b)) => DataCell::Integer(a*b),
            (DataCell::Integer(a), DataCell::Float(b)) => DataCell::Float(f64::from(a)*b),
            (DataCell::Float(a), DataCell::Integer(b)) => DataCell::Float(a*f64::from(*b)),
            (DataCell::Float(a), DataCell::Float(b)) => DataCell::Float(a*b),
            _ => panic!("MUL not implemented")
        }
    }
}

impl Mul<DataCell> for DataCell {
    type Output = DataCell;
    fn mul(self, rhs: DataCell) -> Self::Output {
        self.mul(&rhs)
    }
}
