use crate::channel::{MultiChannelBroadcaster, MultiChannelReader};

/// The interface for ExecutionNode.
///
/// Stream-processes input data and writes output to the output channel. Assumed to
/// process until we consume all data from input_stream.
pub trait StreamProcessor<T: Send>: Send {
    fn process(
        &self,
        input_stream: MultiChannelReader<T>,
        output_stream: MultiChannelBroadcaster<T>,
    );
}
