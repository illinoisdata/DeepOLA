use std::sync::{Weak, RwLock};
use std::convert::AsRef;

use getset::{Getters, Setters};

use crate::ExecutionNode;

pub trait BufferedProcessor {
    fn map(&mut self, input: i64) -> i64 { input }

    fn get_output_partition(&mut self, partition_num: usize) -> Option<i64>;

    fn set_node(&mut self, node: Weak<RwLock<ExecutionNode>>);
}

// Concrete operations that implement the BufferedProcessor trait.
// Operation 1: Reader [] => This is a generator for input data.
#[derive(Getters, Setters)]
pub struct Reader {
    node: Option<Weak<RwLock<ExecutionNode>>>,
    input_data: Vec<i64>,
}

impl Reader {
    pub fn new(input_data: Vec<i64>) -> Self {
        Reader {
            node: None,
            input_data
        }
    }
}

impl BufferedProcessor for Reader {
    fn get_output_partition(&mut self, partition_num: usize) -> Option<i64> {
        // This operation takes the ith input_data (or file_name).
        // Returns the value read (or dataframe).
        let value = self.input_data.get(partition_num);
        if let Some(x) = value {
            Some(*x)
        } else {
            None
        }
    }

    fn map(&mut self, input: i64) -> i64 {
        input
    }

    // Need to have this operation because `dyn BufferedProcessor` cannot do .node = <>
    fn set_node(&mut self, node: Weak<RwLock<ExecutionNode>>) {
        self.node = Some(node);
    }
}

pub enum MapperOp {
    Add,
    Mul
}

// Operation 2: Appender [] => Can have custom map implementations.
#[derive(Getters, Setters)]
pub struct Mapper {
    node: Option<Weak<RwLock<ExecutionNode>>>,
    acc: i64,
    op: MapperOp,
}

impl Mapper {
    pub fn new(acc: i64, op : MapperOp) -> Self {
        Mapper {
            node: None,
            acc,
            op,
        }
    }
}

impl BufferedProcessor for Mapper {
    fn get_output_partition(&mut self, partition_num: usize) -> Option<i64> {
        match &self.node {
            Some(node) => {
                let execution_node = node.upgrade().unwrap();
                let input_partition = execution_node.as_ref().read().unwrap().get_input_partition(0, partition_num);
                match input_partition {
                    Some(a) => Some(self.map(a)),
                    None => None
                }
            },
            None => panic!("ExecutionNode not created!")
        }
    }

    fn map(&mut self, input: i64) -> i64 {
        match self.op {
            MapperOp::Add => { self.acc += input; },
            MapperOp::Mul => { self.acc *= input; },
        }
        self.acc
    }

    fn set_node(&mut self, node: Weak<RwLock<ExecutionNode>>) {
        self.node = Some(node);
    }
}
