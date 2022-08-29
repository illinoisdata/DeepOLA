use crate::data::{DataMessage, Payload};

use super::StreamProcessor;

/// StreamProcessor for a single channel input.
pub trait MessageProcessor<T> {
    fn process_msg(&self, input: &T) -> Option<T>;
}

/// Implements [StreamProcessor] for a type R of trait [MessageProcessor]
impl<T: Send, R: MessageProcessor<T> + Send> StreamProcessor<T> for R {
    fn process_stream(
        &self,
        input_stream: crate::channel::MultiChannelReader<T>,
        output_stream: crate::channel::MultiChannelBroadcaster<T>,
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
                    if let Some(df_acc) = self.process_msg(data_block.data()) {
                        let message = DataMessage::from(df_acc);
                        output_stream.write(message);
                    }
                }
                Payload::Signal(_) => {
                    break;
                }
            }
        }
    }
}

/// Transforms an input from a single channel
pub struct SimpleMapper<T> {
    data_map: Box<dyn Fn(&T) -> Option<T>>,
}

impl<T: Clone> SimpleMapper<T> {
    pub fn identity() -> Self {
        SimpleMapper {
            data_map: Box::new(|x| Some(x.clone())),
        }
    }
}

impl<T> SimpleMapper<T> {
    pub fn ignore() -> Self {
        SimpleMapper {
            data_map: Box::new(|_| None),
        }
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

impl<T: Send> MessageProcessor<T> for SimpleMapper<T> {
    fn process_msg(&self, data: &T) -> Option<T> {
        (self.data_map)(data)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::data::KeyValue;

    use super::*;

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
        let output = mapper.process_msg(&kv_set).unwrap();
        assert_eq!(output.key(), &"mykey".to_string());
        assert_eq!(output.value(), &"hello illinois".to_string());
    }

    #[test]
    fn can_send() {
        let set_processor = Box::new(SimpleMapper::<String>::from(|r: &String| Some(r.clone())));
        thread::spawn(move || {
            // having `drop` prevents warning.
            drop(set_processor);
        });
    }
}
