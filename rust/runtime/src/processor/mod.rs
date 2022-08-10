/// StreamProcessor<T> is the most generic trait.
///
/// For convenience, this module also provides the SimpleRecordProcessor<T> struct, which
/// implements StreamProcessor<T>.

mod stream_processor;

pub use stream_processor::*;