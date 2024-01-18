use std::{sync::{Arc, RwLock}, cell::RefCell};
use getset::{Getters,Setters};

use crate::BufferedProcessor;

#[derive(Getters, Setters)]
pub struct ExecutionNode {
    parents: Vec<Arc<RwLock<ExecutionNode>>>,
    operation: Arc<RefCell<Box<dyn BufferedProcessor>>>,
}

unsafe impl Send for ExecutionNode {}
unsafe impl Sync for ExecutionNode {}

impl ExecutionNode {
    pub fn new(operation: Box<dyn BufferedProcessor>) -> Arc<RwLock<ExecutionNode>> {
        let execution_node = Arc::new(RwLock::new(
            ExecutionNode {
                parents: vec![],
                operation: Arc::new(RefCell::new(operation)),
        }));
        execution_node.as_ref().read().unwrap().operation.as_ref().borrow_mut().set_node(Arc::downgrade(&execution_node));
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
            let result = parent_node_rc.as_ref().read().unwrap().get_output_partition(partition_num);
            result
        }
    }

    // Connect nodes through parent pointers.
    pub fn subscribe_to_node(&mut self, parent: &Arc<RwLock<ExecutionNode>>) {
        self.parents.push(parent.clone());
    }

    pub fn get_all_results(&self) -> Vec<i64> {
        let mut count = 0;
        let mut output = vec![];
        loop {
            let result = self.get_output_partition(count);
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
        let input_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let expected_add_result = vec![1, 3, 6, 10, 15, 21, 28, 36, 45, 55];

        // Creating Operations
        let reader = Box::new(Reader::new(input_data));
        let add = Box::new(Mapper::new(0, MapperOp::Add));

        // Creating Nodes from these operations
        let reader_node = ExecutionNode::new(reader);
        let add_node = ExecutionNode::new(add);

        // Connecting the created nodes.
        add_node.as_ref().write().unwrap().subscribe_to_node(&reader_node);

        // Obtain the result from the final node in the execution graph one by one.
        let mut count = 0;
        loop {
            let result = add_node.as_ref().read().unwrap().get_output_partition(count);
            match result {
                Some(x) => { assert_eq!(x, *expected_add_result.get(count).unwrap()); },
                None => { break; }
            }
            count += 1;
        }
    }

    #[test]
    fn test_multiple_nodes_in_parallel() {
        let input_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let expected_add_result = vec![1, 3, 6, 10, 15, 21, 28, 36, 45, 55];
        let expected_mul_result = vec![1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800];

        let reader = Box::new(Reader::new(input_data));
        let add = Box::new(Mapper::new(0, MapperOp::Add));
        let mul = Box::new(Mapper::new(1, MapperOp::Mul));

        let reader_node = ExecutionNode::new(reader);
        let add_node = ExecutionNode::new(add);
        let mul_node = ExecutionNode::new(mul);

        // Connect the nodes. write() since mut.
        add_node.as_ref().write().unwrap().subscribe_to_node(&reader_node);
        mul_node.as_ref().write().unwrap().subscribe_to_node(&reader_node);

        let handle_1 = thread::spawn(move || {
            add_node.as_ref().read().unwrap().get_all_results()
        });

        let handle_2 = thread::spawn(move || {
            mul_node.as_ref().read().unwrap().get_all_results()
        });

        assert_eq!(handle_1.join().unwrap(), expected_add_result);
        assert_eq!(handle_2.join().unwrap(), expected_mul_result);
    }

}