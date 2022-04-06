use crate::data::*;
use crate::processor::*;

use std::rc::Rc;
use getset::{Getters, Setters};
use nanoid::nanoid;
use std::cell::{RefCell};

use crate::channel::*;
use super::node_base::*;

/// (input channel) -> [This Node] -> (output channels)
///
/// Note that there can be multiple output channels.
///
/// The input channel is the channel that provides the input records for this node
/// to process.
#[derive(Getters, Setters)]
pub struct ExecutionNode<T: Send> {
    #[getset(get)]
    stream_processor: Box<dyn StreamProcessor<T>>,

    input_reader: RefCell<MultiChannelReader<T>>,

    self_writers: Vec<ChannelWriter<T>>,

    /// Once we process records, we send them via these writers.
    ///
    /// `RefCell` makes it possible to treat `ExecutionNode` as immutable when we add
    /// additional output channels.
    output_writer: RefCell<MultiChannelBroadcaster<T>>,

    #[getset(get = "pub")]
    node_id: String,
}

unsafe impl<T: Send> Send for ExecutionNode<T> {}

impl<T: Send> Subscribable<T> for ExecutionNode<T> {
    fn add(&self, channel_writer: ChannelWriter<T>) {
        self.output_writer.borrow_mut().push(channel_writer);
    }
}

impl<T: Send + 'static> ExecutionNode<T> {
    /// Obtains a clone of self_writer. A caller of this method can then write messages to
    /// this node using the obtained writer. This is useful for testing. Why not simply use
    /// another method `write_to_self()`? Obtaining a cloned writer is useful when we need to
    /// **move** this node into a thread. Naturally, moving a node makes its writer not directly
    /// accessible; thus, obtaining a clone of this writer can be useful. We have a test case
    /// using this method.
    pub fn self_writers(&self) -> Vec<ChannelWriter<T>> {
        self.self_writers.clone()
    }

    pub fn self_writer(&self, seq_no: usize) -> ChannelWriter<T> {
        (&self.self_writers[seq_no]).clone()
    }

    pub fn set_data_processor(&mut self, processor: Box<dyn SetProcessorV1<T>>) {
        let stream_processor = SimpleStreamProcessor::<T>::from(processor);
        self.stream_processor = Box::new(stream_processor);
    }

    pub fn set_simple_map(&mut self, map: SimpleMapper<T>) {
        self.set_data_processor(Box::new(map));
    }

    /// This is a convenience method mostly for testing. That is, we directly write a record
    /// into the input channel of this node. In most cases, the records are sent from the
    /// node that this node is subscribed to.
    pub fn write_to_self(&self, channel_no: usize, message: DataMessage<T>) {
        (&self.self_writers[channel_no]).write(message)
    }

    pub fn subscribe_to_node(&self, source_node: &dyn Subscribable<T>, for_channel: usize) {
        let writer = &self.self_writers[for_channel];
        source_node.add(writer.clone());
    }

    /// Processes the data from input stream until we see EOF from all input channels.
    pub fn run(&self) {
        println!("Running Node: {}",self.node_id);
        let input_reader = self.input_reader.borrow();
        let output_writer = self.output_writer.borrow();
        self.stream_processor().process(input_reader.clone(), output_writer.clone());
        println!("Finished Node: {}",self.node_id);
    }

    pub fn input_reader(&self) -> MultiChannelReader<T> {
        self.input_reader.borrow().clone()
    }

    pub fn output_writer(&self) -> MultiChannelBroadcaster<T> {
        self.output_writer.borrow().clone()
    }

    pub fn create() -> Self {
        Self::create_with_record_mapper(SimpleMapper::<T>::from(|_: &T| None))
    }

    /// Convenience method for creating Self from a record mapper. One way is to pass
    /// `SimpleMapper::from_lambda( any closure function )`.
    pub fn create_with_record_mapper(mapper: SimpleMapper<T>) -> Self {
        Self::from_set_processor(Box::new(mapper))
    }

    /// A general factory constructor.
    ///
    /// Takes a set process, which processes a set of T (i.e., `Vec<T>`) and outputs
    /// a possibly empty set of T (which is again `Vec<T>`).
    pub fn from_set_processor(data_processor: Box<dyn SetProcessorV1<T>>) -> Self {
        let stream_processor = SimpleStreamProcessor::<T>::from(data_processor);
        Self::new(Box::new(stream_processor), 1)
    }

    pub fn from_right_complete_processor(data_processor: Box<dyn SetMultiProcessor<T>>, num_channels: usize) -> Self {
        let stream_processor = RightCompleteProcessor::<T>::from(data_processor);
        Self::new(Box::new(stream_processor), num_channels)
    }

    pub fn new_single_input(stream_processor: Box<dyn StreamProcessor<T>>) -> Self {
        Self::new(stream_processor, 1)
    }

    pub fn new_double_inputs(stream_processor: Box<dyn StreamProcessor<T>>) -> Self {
        Self::new(stream_processor, 2)
    }

    pub fn new(stream_processor: Box<dyn StreamProcessor<T>>, num_input: usize) -> Self {
        let mut input_channels = MultiChannelReader::<T>::new();
        let mut self_writers = vec![];
        for _ in 0..num_input {
            let (write_channel, read_channel) = Channel::create::<T>();
            input_channels.push(Rc::new(read_channel));
            self_writers.push(write_channel);
        }

        Self {
            stream_processor,
            input_reader: RefCell::new(input_channels),
            self_writers,
            output_writer: RefCell::new(MultiChannelBroadcaster::<T>::new()),
            node_id: nanoid!(NODE_ID_LEN, &NODE_ID_ALPHABET),
        }
    }
}

impl<T: Send + 'static> From<Box<dyn SetMultiProcessor<T>>> for ExecutionNode<T> {
    fn from(data_processor: Box<dyn SetMultiProcessor<T>>) -> Self {
        let stream_processor = RightCompleteProcessor::<T>::from(data_processor);
        Self::new(Box::new(stream_processor), 1)
    }
}

impl<T: Send + 'static> From<(Box<dyn SetMultiProcessor<T>>,usize)> for ExecutionNode<T> {
    fn from(input: (Box<dyn SetMultiProcessor<T>>,usize)) -> Self {
        let stream_processor = RightCompleteProcessor::<T>::from(input.0);
        Self::new(Box::new(stream_processor), input.1)
    }
}


pub struct NodeReader<T: Send> {
    /// We use the channel of this node to listens to the node we want to read from.
    /// We just need to a single input channel.
    internal_node: ExecutionNode<T>,
}

impl<T: Send + 'static> NodeReader<T> {
    pub fn read(&self) -> DataMessage<T> {
        self.internal_node.input_reader().read(0)
    }

    pub fn new(listens_to: &ExecutionNode<T>) -> Self {
        let node = ExecutionNode::create();
        node.subscribe_to_node(listens_to, 0);
        Self {
            internal_node: node,
        }
    }

    pub fn empty() -> Self {
        Self {
            internal_node: ExecutionNode::create(),
        }
    }

    pub fn subscribe_to_node(&mut self, listens_to: &ExecutionNode<T>, for_channel: usize) {
        self.internal_node.subscribe_to_node(listens_to, for_channel);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::processor::SimpleMapper;
    use std::{time, thread};

    /// ctor runs this `init()` function for each test case.
    #[ctor::ctor]
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn can_create_node() {
        let node = ExecutionNode::create();
        let node_reader = NodeReader::new(&node);
        node.write_to_self(0, DataMessage::from_single(KeyValue::from_str(
            "mykey", "hello",
        )));
        node.write_to_self(0, DataMessage::eof());
        node.run();
        node_reader.read();
    }

    #[test]
    fn can_move_to_thread() {
        let node = ExecutionNode::<String>::create();
        node.write_to_self(0, DataMessage::from_single("hello".to_string()));
        node.write_to_self(0, DataMessage::eof());
        thread::spawn(move || {
            node.run();
        });
    }

    /// The self_writer can be used even after **moving** a node into a thread.
    #[test]
    fn can_keep_self_writer() {
        let node = ExecutionNode::<String>::create();
        let self_writer = node.self_writer(0);
        thread::spawn(move || {
            node.run(); // at this point, no data exists.
        });
        self_writer.write(DataMessage::from_single("hello".to_string()));
        self_writer.write(DataMessage::eof());
    }

    #[test]
    fn can_stop() {
        let node = ExecutionNode::<String>::create();
        let self_writer = node.self_writer(0);
        let handle = thread::spawn(move || {
            node.run(); // at this point, no data exists, but keeps waiting.
        });
        let ten_millis = time::Duration::from_millis(10);
        let now = time::Instant::now();
        thread::sleep(ten_millis);
        self_writer.write(DataMessage::stop());
        handle.join().unwrap();
        assert!(now.elapsed() >= ten_millis);
    }

    /// The source node's output channel and the target node's input channel have the
    /// same channel_id (because they are connected via the channel).
    #[test]
    fn channel_ids_match() {
        let node1 = ExecutionNode::<KeyValue>::create();
        let node2 = ExecutionNode::<KeyValue>::create();
        node2.subscribe_to_node(&node1, 0);
        let node1_out_channel = node1.output_writer();
        let node2_in_channel = node2.input_reader();
        assert_eq!(node1_out_channel.len(), 1);
        assert_eq!(
            node1_out_channel.writer(0).channel_id(),
            node2_in_channel.reader(0).channel_id()
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
            node.set_simple_map(SimpleMapper::<KeyValue>::from(|r: &KeyValue| {
                Some(KeyValue::from_string(
                    r.key().into(),
                    r.value().to_string() + "X",
                ))
            }));
            if i > 0 {
                let prev_node = &node_list[i - 1];
                node.subscribe_to_node(prev_node, 0);
            }
            node_list.push(node);
        }
        let first_node = &node_list[0];
        let last_node = &node_list[node_list.len() - 1];
        let reader_node = NodeReader::new(last_node);
        first_node.write_to_self(0,
            DataMessage::from_single(KeyValue::from_str("mykey", "")));
        first_node.write_to_self(0, DataMessage::eof());

        // process one by one
        for node in node_list.iter() {
            node.run();
        }

        let out_msg = reader_node.read().payload();
        if let Payload::Some(data_arc) = out_msg {
            let data = data_arc.data();
            assert_eq!(data.len(), 1);
            let kv = &data[0];
            assert_eq!(kv.key(), &"mykey".to_string());
            assert_eq!(
                kv.value(),
                &(0..loop_count).map(|_| "X").collect::<String>()
            );
        } else {
            panic!("message not retrieved.");
        }
    }
}
