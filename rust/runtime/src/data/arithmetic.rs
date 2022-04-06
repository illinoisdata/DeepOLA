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

impl Add<&Box<DataCell>> for Box<DataCell> {
    type Output = Box<DataCell>;
    fn add(self, rhs: &Box<DataCell>) -> Self::Output {
        Box::new(*self + *rhs.clone())
    }
}

impl Add<Box<DataCell>> for Box<DataCell> {
    type Output = Box<DataCell>;
    fn add(self, rhs: Box<DataCell>) -> Self::Output {
        self.add(&rhs)
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

#[cfg(test)]
mod tests {
    use super::DataCell;

    #[test]
    fn can_add_datacell() {
        let int1 = DataCell::Integer(1);
        let int2 = DataCell::Integer(2);
        let float1 = DataCell::Float(4.0);
        let float2 = DataCell::Float(2.5);
        assert_eq!(int1.clone()+int2.clone(), DataCell::Integer(3));
        assert_eq!(int1.clone()+float1.clone(), DataCell::Float(5.0));
        assert_eq!(float1.clone()+float2.clone(), DataCell::Float(6.5));
    }

    #[test]
    fn can_sub_datacell() {
        let int1 = DataCell::Integer(1);
        let int2 = DataCell::Integer(2);
        let float1 = DataCell::Float(4.0);
        let float2 = DataCell::Float(2.5);
        assert_eq!(int1.clone()-int2.clone(), DataCell::Integer(-1));
        assert_eq!(int1.clone()-float1.clone(), DataCell::Float(-3.0));
        assert_eq!(float1.clone()-float2.clone(), DataCell::Float(1.5));
    }

    #[test]
    fn can_mul_datacell() {
        let int1 = DataCell::Integer(1);
        let int2 = DataCell::Integer(2);
        let float1 = DataCell::Float(4.0);
        let float2 = DataCell::Float(2.5);
        assert_eq!(int1.clone()*int2.clone(), DataCell::Integer(2));
        assert_eq!(int1.clone()*float1.clone(), DataCell::Float(4.0));
        assert_eq!(float1.clone()*float2.clone(), DataCell::Float(10.0));
    }

    #[test]
    fn can_div_datacell() {
        let int1 = DataCell::Integer(1);
        let int2 = DataCell::Integer(2);
        let float1 = DataCell::Float(4.0);
        let float2 = DataCell::Float(2.5);
        assert_eq!(int1.clone()/int2.clone(), DataCell::Float(0.5));
        assert_eq!(int1.clone()/float1.clone(), DataCell::Float(0.25));
        assert_eq!(float1.clone()/float2.clone(), DataCell::Float(4.0/2.5));
    }
}