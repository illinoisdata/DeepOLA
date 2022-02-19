use getset::Getters;
use std::thread::{self, JoinHandle};

use super::node::ExecutionNode;

#[derive(Getters)]
pub struct ExecutionService<T: Send> {
    #[getset(get = "pub")]
    nodes: Vec<ExecutionNode<T>>,

    thread_handles: Vec<JoinHandle<ExecutionNode<T>>>,
}

impl<T: Send + 'static> ExecutionService<T> {
    /// Register a node to execute. Note that the registered node is
    /// **owned** by this service now.
    pub fn add(&mut self, node: ExecutionNode<T>) {
        self.nodes.push(node);
    }

    fn aseert_empty_thread_handles(&self) {
        if !self.thread_handles.is_empty() {
            panic!("There are {} thread handles.", self.thread_handles.len());
        }
    }

    pub fn run(&mut self) {
        self.aseert_empty_thread_handles();

        self.thread_handles.clear();
        while let Some(node) = self.nodes.pop() {
            let handle = thread::spawn(move || {
                node.run();
                node
            });
            self.thread_handles.push(handle);
        }
    }

    pub fn join(&mut self) {
        while let Some(handle) = self.thread_handles.pop() {
            handle.join().unwrap();
        }
    }

    pub fn create() -> Self {
        ExecutionService {
            nodes: vec![],
            thread_handles: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{data::message::DataMessage, graph::node::ExecutionNode, processor::SimpleMapper};

    use super::ExecutionService;

    #[test]
    fn stop_given_eof() {
        let node =
            ExecutionNode::create_with_record_mapper(SimpleMapper::from_lambda(|r: &String| {
                Some(r.clone() + "X")
            }));
        let self_writer = node.self_writer();
        let mut exec_service = ExecutionService::create();
        exec_service.add(node);
        exec_service.run();

        // without this line, this test case doesn't stop, looping infinitely.
        self_writer.write(DataMessage::eof());

        exec_service.join();
    }
}
