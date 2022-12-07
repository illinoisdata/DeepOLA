use std::{cell::RefCell, sync::Arc};
use getset::{Getters,Setters};

use crate::BufferedProcessor;

#[derive(Getters, Setters)]
pub struct ExecutionNode {
    node_id: usize,
    parents: Vec<Arc<RefCell<ExecutionNode>>>,
    progress: usize,
    operation: Arc<RefCell<Box<dyn BufferedProcessor>>>,
}

unsafe impl Send for ExecutionNode {}
unsafe impl Sync for ExecutionNode {}

impl ExecutionNode {
    pub fn new(operation: Box<dyn BufferedProcessor>) -> Arc<RefCell<ExecutionNode>> {
        let execution_node = Arc::new(RefCell::new(
            ExecutionNode {
                node_id: 0,
                parents: vec![],
                progress: 0,
                operation: Arc::new(RefCell::new(operation)),
        }));
        execution_node.as_ref().borrow().operation.as_ref().borrow_mut().set_node(Arc::downgrade(&execution_node));
        execution_node
    }

    pub fn get(&mut self) -> Option<i64> {
        todo!("User facing interface that allows to obtain the result for next partition");
    }

    pub fn run(&mut self) {
        todo!("Run execution of all available partitions");
    }

    // Node obtains ith output operation by invoking the operation's get_output_partition.
    pub fn get_output_partition(&self, partition_num: usize) -> Option<i64> {
        let result = self.operation.as_ref().borrow_mut().get_output_partition(partition_num);
        result
    }

    // Interface to obtain `partition_num` partition from the parent node at `seq_no`.
    // The operation panics if no such parent node is available else returns the partition.
    pub fn get_input_partition(&self, seq_no: usize, partition_num: usize) -> Option<i64> {
        if seq_no >= self.parents.len()  {
            panic!("No parent node at seq_no: {}", seq_no);
        } else {
            let parent_node_rc = self.parents.get(seq_no).unwrap();
            let result = parent_node_rc.as_ref().borrow().get_output_partition(partition_num);
            result
        }
    }

    // Connect nodes through parent pointers.
    pub fn subscribe_to_node(&mut self, parent: &Arc<RefCell<ExecutionNode>>) {
        self.parents.push(parent.clone());
    }

    pub fn get_all_results(&self) -> Vec<i64> {
        let mut count = 0;
        let mut output = vec![];
        loop {
            let result = self.get_output_partition(count);
            println!("Node: {}, Result: {:?}", self.node_id, result);
            match result {
                Some(x) => {output.push(x); count += 1;},
                None => {break;}
            }
        }
        output
    }

}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::*;

    #[test]
    fn test_single_mapper_node() {
        // Creating Operations
        let reader = Box::new(Reader::new(vec![1,2,3,4,5,6,7,8,9,10]));
        let adder = Box::new(Mapper::new(0, MapperOp::Add));

        // Creating Nodes from these operations
        let reader_node = ExecutionNode::new(reader);
        let adder_node = ExecutionNode::new(adder);

        // Connecting the created nodes.
        adder_node.as_ref().borrow_mut().subscribe_to_node(&reader_node);

        // Obtain the result from the final node in the execution graph.
        let expected_result = vec![1, 3, 6, 10, 15, 21, 28, 36, 45, 55];
        let mut count = 0;
        loop {
            let result = adder_node.as_ref().borrow().get_output_partition(count);
            match result {
                Some(x) => { assert_eq!(x, *expected_result.get(count).unwrap()); },
                None => { break; }
            }
            count += 1;
        }
    }
}