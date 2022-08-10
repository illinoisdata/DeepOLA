use crate::{channel::{MultiChannelBroadcaster, MultiChannelReader}, data::{Payload, DataMessage, DataBlock}};

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

/// Transforms an input from a single channel
pub struct SimpleMapper<T> {
    data_map: Box<dyn Fn(&T) -> Option<T>>,
}

impl<T> SimpleMapper<T> {
    pub fn process(&self, data: &T) -> Option<T> {
        (self.data_map)(data)
    }
}

impl<T: Send, F> From<F> for SimpleMapper<T>
where
    F: Fn(&T) -> Option<T> + 'static,
{
    /// Constructor from a closure
    fn from(record_map: F) -> Self {
        SimpleMapper {
            data_map: Box::new(record_map),
        }
    }
}

unsafe impl<T> Send for SimpleMapper<T> {}

impl<T: Send> StreamProcessor<T> for SimpleMapper<T> {
    fn process(
        &self,
        input_stream: MultiChannelReader<T>,
        output_stream: MultiChannelBroadcaster<T>,
    ) {
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            match message.payload() {
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Some(data_block) => {
                    let output = self.process(data_block.data());
                    if output.is_some() {
                        let message = DataMessage::from(DataBlock::from(output.unwrap()));
                        output_stream.write(message);
                    }
                },
                Payload::Signal(_) => {
                    break;
                },
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use std::thread;

    use crate::{data::{KeyValue}, processor::{SimpleMapper}};

    #[test]
    fn test_closure() {
        let kv_set = KeyValue::from_str("mykey", "hello");
        let my_name = "illinois"; // this variable is captured
        let mapper = SimpleMapper::from(|a: &KeyValue| {
            Some(KeyValue::from_string(
                a.key().into(),
                a.value().to_string() + " " + my_name,
            ))
        });
        let output = mapper.process(&kv_set).unwrap();
        assert_eq!(output.key(), &"mykey".to_string());
        assert_eq!(output.value(), &"hello illinois".to_string());
    }

    #[test]
    fn can_send() {
        let set_processor =
            Box::new(SimpleMapper::<String>::from(|r: &String| Some(r.clone())));
        thread::spawn(move || {
            // having `drop` prevents warning.
            drop(set_processor);
        });
    }
}
