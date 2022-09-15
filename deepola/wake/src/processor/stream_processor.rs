use crate::channel::{MultiChannelBroadcaster, MultiChannelReader};

/// The interface for ExecutionNode.
///
/// Stream-processes input data and writes output to the output channel. Assumed to
/// process until we consume all data from input_stream.
pub trait StreamProcessor<T: Send>: Send {
    /// This function is called before processing actual data. Useful to support the case where
    /// a node needs pre-processing in advance.
    fn pre_process(&mut self, _input_stream: MultiChannelReader<T>) {}

    /// This function actually processes data.
    fn process_stream(
        &self,
        input_stream: MultiChannelReader<T>,
        output_stream: MultiChannelBroadcaster<T>,
    );
}
