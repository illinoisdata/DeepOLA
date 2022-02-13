use crate::data::message::*;
use crate::data::payload::{Payload, Signal};
use crate::processor::{SetProcessor, SimpleMapper};

use core::time;
use getset::{Getters, Setters};
use nanoid::nanoid;
use std::cell::{Ref, RefCell};
use std::collections::VecDeque;
use std::thread::{self, JoinHandle};

use super::channel::*;

/// (input channel) -> [This Node] -> (output channels)
///
/// Note that there can be multiple output channels.
///
/// The input channel is the channel that provides the input records for this node
/// to process.
#[derive(Getters, Setters)]
pub struct ExecutionNode<T: Send> {
    #[getset(get)]
    data_processor: Box<dyn SetProcessor<T>>,

    #[getset(get = "pub")]
    channel_reader: ChannelReader<T>,

    self_writer: ChannelWriter<T>,

    /// Once we process records, we send them via these writers.
    /// 
    /// `RefCell` makes it possible to treat `ExecutionNode` as immutable when we add
    /// additional output channels.
    output_channels: RefCell<Vec<ChannelWriter<T>>>,

    #[getset(get = "pub")]
    node_state: RefCell<NodeState>,

    #[getset(get = "pub")]
    node_id: String,
}

unsafe impl<T: Send> Send for ExecutionNode<T> {}

#[derive(PartialEq)]
pub enum NodeState {
    /// A thread is not running.
    STOPPED,

    /// A signal is set to stop as early as possible.
    STOPPING,

    /// A tread is running to process input records as quickly as possible.
    RUNNING,
}

impl<T: Send + 'static> ExecutionNode<T> {

    /// Obtains a clone of self_writer. A caller of this method can then write messages to
    /// this node using the obtained writer. This is useful for testing. Why not simply use
    /// another method `write_to_self()`? Obtaining a cloned writer is useful when we need to
    /// **move** this node into a thread. Naturally, moving a node makes its writer not directly
    /// accessible; thus, obtaining a clone of this writer can be useful. We have a test case
    /// using this method.
    pub fn self_writer(&self) -> ChannelWriter<T> {
        self.self_writer.clone()
    }

    pub fn set_processor(&mut self, processor: Box<dyn SetProcessor<T>>) {
        self.data_processor = processor;
    }

    pub fn set_simple_map(&mut self, map: SimpleMapper<T>) {
        self.set_processor(Box::new(map));
    }

    /// This is a convenience method mostly for testing. That is, we directly write a record
    /// into the input channel of this node. In most cases, the records are sent from the
    /// node that this node is subscribed to.
    pub fn write_to_self(&self, record: DataMessage<T>) {
        let writer = self.self_writer();
        writer.write(record);
    }

    pub fn subscribe_to_node(&self, source_node: &ExecutionNode<T>) {
        source_node
            .output_channels
            .borrow_mut()
            .push(self.self_writer.clone());
    }

    /// This is an internal function used to process a single data item. In practice, this
    /// function must be executed inside an infinite loop until there is no more data items.
    /// This function returns immediately if there are no items to process.
    pub fn process_payload(&self) -> ExecStatus {
        let message_optional = self.channel_reader.read();
        match message_optional {
            None => ExecStatus::EmptyChannel, // No message is read; thus, nothing processed.
            Some(message) => match message.payload() {
                Payload::EOF => {
                    self.write_to_all_writers(&DataMessage::eof());
                    log::info!("EOF: (Channel: {}) -> [Node: {}] -> (Channel: {})",
                        self.channel_reader().channel_id(),
                        self.node_id(),
                        self.output_channel_ids(),
                    );
                    ExecStatus::EOF
                }
                Payload::Some(dblock) => {
                    let output = DataMessage::from_data_block(self.data_processor().process(&dblock));
                    self.write_to_all_writers(&output);
                    log::info!(
                        "Processed: (Channel: {}; {} records) -> [Node: {}] -> (Channel: {}; {} records)",
                        self.channel_reader().channel_id(),
                        message.len(),
                        self.node_id(),
                        self.output_channel_ids(),
                        output.len(),
                    );
                    ExecStatus::Ok(output.len())
                }
                Payload::Signal(s) => {
                    log::info!("{:?}: (Channel: {})", s, self.channel_reader().channel_id());
                    self.process_signal(s);
                    ExecStatus::Ok(0)
                }
            },
        }
    }

    fn output_channels(&self) -> Ref<'_, Vec<ChannelWriter<T>>> {
        self.output_channels.borrow()
    }

    fn output_channel_ids(&self) -> String {
        let output_channels = self.output_channels();
        if output_channels.len() == 0 {
            return "empty".to_string();
        }
        let mut composed: String = output_channels[0].channel_id().clone();
        for i in 1..output_channels.len() {
            composed += ", ";
            composed += &output_channels[i].channel_id().clone();
        }
        return composed;
    }

    pub fn process_signal(&self, s: Signal) {
        match s {
            Signal::STOP => self.set_state(NodeState::STOPPING),
        }
    }

    fn set_state(&self, state: NodeState) {
        self.node_state.replace(state);
    }

    /// Repetitively executes [`process_payload()`].
    ///
    /// This method is designed to be called in a thread.
    pub fn run(&self) {
        self.set_state(NodeState::RUNNING);
        loop {
            if self.node_state().borrow().eq(&NodeState::STOPPING) {
                break;
            }
            let status = self.process_payload();
            match status {
                ExecStatus::EOF => break,
                ExecStatus::EmptyChannel => (),
                ExecStatus::Ok(_) => (),
                ExecStatus::Err(msg) => panic!("{}", msg),
            }
            self.sleep_micro_secs(NODE_SLEEP_MICRO_SECONDS);
        }
        self.set_state(NodeState::STOPPED);
    }

    fn sleep_micro_secs(&self, micro_secs: u64) {
        let sleep_micros = time::Duration::from_micros(micro_secs);
        thread::sleep(sleep_micros);
    }

    fn write_to_all_writers(&self, message: &DataMessage<T>) {
        for writer in self.output_channels.borrow().iter() {
            writer.write(message.clone());
        }
    }

    pub fn create() -> Self {
        Self::create_with_record_mapper(SimpleMapper::from_lambda(|_| None))
    }

    /// Convenience method for creating Self from a record mapper. One way is to pass
    /// `SimpleMapper::from_lambda( any closure function )`.
    pub fn create_with_record_mapper(mapper: SimpleMapper<T>) -> Self {
        Self::create_with_set_processor(Box::new(mapper))
    }

    /// A general factory constructor.
    /// 
    /// Takes a set process, which processes a set of T (i.e., `Vec<T>`) and outputs 
    /// a possibly empty set of T (which is again `Vec<T>`).
    pub fn create_with_set_processor(data_processor: Box<dyn SetProcessor<T>>) -> Self {
        let (write_channel, read_channel) = Channel::create::<T>();
        Self {
            data_processor,
            channel_reader: read_channel,
            self_writer: write_channel,
            output_channels: RefCell::new(vec![]),
            node_state: RefCell::new(NodeState::STOPPED),
            node_id: nanoid!(NODE_ID_LEN, &NODE_ID_ALPHABET),
        }
    }
}

const NODE_ID_ALPHABET: [char; 16] = [
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f',
];

const NODE_ID_LEN: usize = 5;

pub enum ExecStatus {
    Ok(usize),
    EmptyChannel,
    EOF,
    Err(String),
}

const NODE_SLEEP_MICRO_SECONDS: u64 = 1;

#[derive(Getters)]
pub struct ExecutionService<T: Send> {
    #[getset(get="pub")]
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
        ExecutionService { nodes: vec![], thread_handles: vec![] }
    }

}

pub struct NodeReader<T: Send> {
    /// We use the channel of this node to listens to the node we want to read from.
    internal_node: ExecutionNode<T>,
}

impl<T: Send + 'static> NodeReader<T> {
    pub fn read(&self) -> Option<DataMessage<T>> {
        self.internal_node.channel_reader.read()
    }

    pub fn create(listens_to: &ExecutionNode<T>) -> Self {
        let node = ExecutionNode::create();
        node.subscribe_to_node(listens_to);
        Self {
            internal_node: node,
        }
    }
}

pub struct InOutTracker<T: Send + Sync> {
    last_consumed: RefCell<VecDeque<DataMessage<T>>>,
    last_produced: RefCell<VecDeque<DataMessage<T>>>,
    capacity_limit: usize,
}

impl<T: Send + Sync> InOutTracker<T> {
    pub fn add_to_consumed(&self, record: DataMessage<T>) {
        let mut queue = self.last_consumed.borrow_mut();
        queue.push_back(record);
        queue.truncate(self.capacity_limit);
    }

    pub fn add_to_produced(&self, record: DataMessage<T>) {
        let mut queue = self.last_produced.borrow_mut();
        queue.push_back(record);
        queue.truncate(self.capacity_limit);
    }

    pub fn get_last_consumed(&self) -> Ref<VecDeque<DataMessage<T>>> {
        self.last_consumed.borrow()
    }

    pub fn get_last_produced(&self) -> Ref<VecDeque<DataMessage<T>>> {
        self.last_produced.borrow()
    }

    pub fn create() -> Self {
        Self {
            last_consumed: RefCell::new(VecDeque::new()),
            last_produced: RefCell::new(VecDeque::new()),
            capacity_limit: IN_OUT_TRACKER_SIZE,
        }
    }
}

const IN_OUT_TRACKER_SIZE: usize = 100;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::data::kv::KeyValue;
    use crate::data::message::DataMessage;
    use crate::processor::SimpleMapper;
    use std::time;

    /// ctor runs this `init()` function for each test case.
    #[ctor::ctor]
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn can_create_node() {
        let node = ExecutionNode::create();
        let node_reader = NodeReader::create(&node);
        node.write_to_self(DataMessage::from_single(KeyValue::from_str(
            "mykey", "hello",
        )));
        node.process_payload();
        node_reader.read();
    }

    #[test]
    fn can_move_to_thread() {
        let node = ExecutionNode::<String>::create();
        node.write_to_self(DataMessage::from_single("hello".to_string()));
        thread::spawn(move || {
            node.process_payload();
        });
    }

    /// The self_writer can be used even after **moving** a node into a thread.
    #[test]
    fn can_keep_self_writer() {
        let node = ExecutionNode::<String>::create();
        let self_writer = node.self_writer();
        thread::spawn(move || {
            node.process_payload(); // at this point, no data exists.
        });
        self_writer.write(DataMessage::from_single("hello".to_string()));
    }

    #[test]
    fn can_stop() {
        let node = ExecutionNode::<String>::create();
        let self_writer = node.self_writer();
        let handle = thread::spawn(move || {
            node.run(); // at this point, no data exists, but keeps looping.
        });
        let ten_millis = time::Duration::from_millis(10);
        let now = time::Instant::now();
        thread::sleep(ten_millis);
        self_writer.write(DataMessage::stop());
        handle.join().unwrap();
        assert!(now.elapsed() >= ten_millis);
    }

    #[test]
    fn processing_empty_queue_doesnt_block() {
        let node = ExecutionNode::<KeyValue>::create();
        node.process_payload();
    }

    /// The source node's output channel and the target node's input channel have the 
    /// same channel_id (because they are connected via the channel).
    #[test]
    fn channel_ids_match() {
        let node1 = ExecutionNode::<KeyValue>::create();
        let node2 = ExecutionNode::<KeyValue>::create();
        node2.subscribe_to_node(&node1);
        let node1_out_channel = node1.output_channels();
        let node2_in_channel = node2.channel_reader();
        assert_eq!(node1_out_channel.len(), 1);
        assert_eq!(
            node1_out_channel[0].channel_id(),
            node2_in_channel.channel_id()
        )
    }

    /// We create ten linearly connected nodes. Each node adds "X" at the end of the passed
    /// value. Finally, we see ten "X"es added to the value.
    #[test]
    fn can_send_over_multi_hops() {
        let mut node_list: Vec<ExecutionNode<KeyValue>> = vec![];
        let loop_count = 10;
        for i in 0..loop_count {
            let mut node = ExecutionNode::create();
            node.set_simple_map(SimpleMapper::<KeyValue>::from_lambda(|r| {
                Some(KeyValue::from_string(
                    r.key().into(),
                    r.value().to_string() + "X",
                ))
            }));
            if i > 0 {
                let prev_node = &node_list[i - 1];
                node.subscribe_to_node(prev_node);
            }
            node_list.push(node);
        }
        let first_node = &node_list[0];
        let last_node = &node_list[node_list.len() - 1];
        let reader_node = NodeReader::create(last_node);
        first_node
            .self_writer()
            .write(DataMessage::from_single(KeyValue::from_str("mykey", "")));

        // process one by one
        for node in node_list.iter() {
            node.process_payload();
        }

        let out_msg_optional = reader_node.read();
        assert!(out_msg_optional.is_some());
        let out_dataset = out_msg_optional.unwrap().clone();
        assert_eq!(out_dataset.len(), 1);
        let out_kv = &out_dataset[0];
        assert_eq!(out_kv.key(), &"mykey".to_string());
        assert_eq!(
            out_kv.value(),
            &(0..loop_count).map(|_| "X").collect::<String>()
        );
    }

    #[test]
    fn stop_given_eof() {
        let node = ExecutionNode::create_with_record_mapper(
            SimpleMapper::from_lambda(|r: &String| Some(r.clone() + "X") ));
        let self_writer = node.self_writer();
        let mut exec_service = ExecutionService::create();
        exec_service.add(node);
        exec_service.run();

        // without this line, this test case doesn't stop, looping infinitely.
        self_writer.write(DataMessage::eof());

        exec_service.join();
    }
}
