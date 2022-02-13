/// StreamProcessor<T> is the most generic trait.
/// 
/// For convenience, this module also provides the SimpleRecordProcessor<T> struct, which
/// implements StreamProcessor<T>.

mod stream_processor;
mod set_processor;
mod record_processor;

pub use stream_processor::*;
pub use set_processor::*;
pub use record_processor::*;
