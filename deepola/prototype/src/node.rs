use std::{cell::{RefCell, Ref}, rc::{Rc, Weak}, borrow::{Borrow, BorrowMut}};
use std::convert::AsRef;
use getset::{Getters,Setters};

// pub struct ExecutionService {
//     count: usize,
//     nodes: Vec<Rc<RefCell<ExecutionNode>>>,
// }

pub trait BufferedProcessor {
    fn map(&mut self, input: i64) -> i64 { input }

    fn get_output_partition(&mut self, partition_num: usize) -> Option<i64>;

    fn set_node(&mut self, node: Weak<RefCell<ExecutionNode>>);
}

#[derive(Getters, Setters)]
pub struct ExecutionNode {
    node_id: usize,
    parents: Vec<Rc<RefCell<ExecutionNode>>>,
    progress: usize,
    operation: Rc<RefCell<Box<dyn BufferedProcessor>>>,
}

impl ExecutionNode {
    // fn new(operation: Box<dyn BufferedProcessor>) -> Rc<RefCell<ExecutionNode>> {
    //     let execution_node = Rc::new(RefCell::new(
    //         ExecutionNode {
    //             node_id: 0,
    //             parents: vec![],
    //             operation: Rc::clone(&Rc::new(RefCell::new(operation))),
    //         }
    //     ));
    //     operation.as_ref().borrow_mut().set_node(Rc::downgrade(&execution_node));
    //     execution_node
    // }

    // fn get(&mut self, progress) -> Option<i64> {
    //     let next_result = 1 + self.progress;
    //     let result = self.get_output_partition(next_result);
    //     self.progress += 1;
    //     result
    // }

    fn new(operation: Box<dyn BufferedProcessor>) -> Rc<RefCell<ExecutionNode>> {
        let execution_node = Rc::new(RefCell::new(
            ExecutionNode {
            node_id: 0,
            parents: vec![],
            progress: 0,
            operation: Rc::clone(&Rc::new(RefCell::new(operation))),
        }));
        execution_node.as_ref().borrow().operation.as_ref().borrow_mut().set_node(Rc::downgrade(&execution_node));
        execution_node
    }

    fn run(&mut self) {
        println!("Starting Execution");
    }

    fn get_output_partition(&self, partition_num: usize) -> Option<i64> {
        let result = self.operation.as_ref().borrow_mut().get_output_partition(partition_num);
        result
    }

    fn get_input_partition(&self, seq_no: usize, partition_num: usize) -> Option<i64> {
        println!("Getting Input from seq_no: {} when parents are: {}", seq_no, self.parents.len());
        if seq_no >= self.parents.len()  {
            None
        } else {
            let parent_node_rc = self.parents.get(seq_no).unwrap();
            let result = parent_node_rc.as_ref().borrow().get_output_partition(partition_num);
            result
        }
    }

    fn subscribe_to_node(&mut self, parent: &Rc<RefCell<ExecutionNode>>) {
        self.parents.push(parent.clone());
    }

}

// Concrete operations that implement the BufferedProcessor trait.

// Operation 1: Reader [] => This is a generator for input data.
pub struct Reader {
    node: Option<Weak<RefCell<ExecutionNode>>>,
    input_data: Vec<i64>,
}
impl BufferedProcessor for Reader {
    fn get_output_partition(&mut self, partition_num: usize) -> Option<i64> {
        println!("GET OUTPUT PARTITION CALLED FOR READER");
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

    fn set_node(&mut self, node: Weak<RefCell<ExecutionNode>>) {
        self.node = Some(node);
    }
}

pub enum MapperOp {
    Add,
    Mul
}

// Operation 2: Appender [] => Can have custom map implementations.
pub struct Mapper {
    node: Option<Weak<RefCell<ExecutionNode>>>,
    acc: i64,
    op: MapperOp,
}
impl BufferedProcessor for Mapper {
    fn get_output_partition(&mut self, partition_num: usize) -> Option<i64> {
        println!("GET OUTPUT PARTITION CALLED FOR MAPPER");
        match &self.node {
            Some(node) => {
                let execution_node = node.upgrade().unwrap();
                let input_partition = execution_node.as_ref().borrow().get_input_partition(0, partition_num);
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

    fn set_node(&mut self, node: Weak<RefCell<ExecutionNode>>) {
        self.node = Some(node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        println!("Test Runs");
        let reader = Box::new(Reader { node: None, input_data : vec![1,2,3,4,5,6,7,8,9,10]});
        let adder = Box::new(Mapper { node: None, acc: 0, op: MapperOp::Add});
        let multiplier = Box::new(Mapper { node: None, acc: 1, op: MapperOp::Mul});

        let reader_node = ExecutionNode::new(reader); // Specify lazy mode
        let adder_node = ExecutionNode::new(adder);
        let multiplier_node = ExecutionNode::new(multiplier); // Specify lazy mode

        adder_node.as_ref().borrow_mut().subscribe_to_node(&reader_node);
        multiplier_node.as_ref().borrow_mut().subscribe_to_node(&reader_node);

        println!("Nodes Created. Subscriptions Done.");

        let mut count = 0;
        loop {
            let result = adder_node.as_ref().borrow().get_output_partition(count);
            // let result = multiplier_node.as_ref().borrow().get_output_partition(count);
            match result {
                Some(x) => { println!("Result: {:?}", x); },
                None => { break; }
            }
            count += 1;
        }
    }

}