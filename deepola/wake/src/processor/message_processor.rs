use std::collections::HashMap;
use crate::data::{
    DATABLOCK_CARDINALITY,
    DataBlock,
    DataMessage,
    MetaCell,
    Payload,
};

use super::StreamProcessor;

/// StreamProcessor for a single channel input.
pub trait MessageProcessor<T> {
    fn process_msg(&self, input: &T) -> Option<T>;

    fn post_process_msg(&self) -> Option<T> {
        None
    }
}

/// Implements [StreamProcessor] for a type R of trait [MessageProcessor]
impl<T: Send, R: MessageProcessor<T> + Send> StreamProcessor<T> for R {
    fn process_stream(
        &self,
        input_stream: crate::channel::MultiChannelReader<T>,
        output_stream: crate::channel::MultiChannelBroadcaster<T>,
    ) {
        let mut start_time = std::time::Instant::now();
        let mut last_metadata: Option<HashMap<String, MetaCell>> = None;
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            log::info!(
                "[logging] type=execution thread={:?} action=read time={:?}",
                std::thread::current().id(),
                start_time.elapsed().as_micros()
            );
            start_time = std::time::Instant::now();
            match message.payload() {
                Payload::EOF => {
                    if let Some(df_acc) = self.post_process_msg() {
                        let mut eof_metadata = last_metadata.as_ref()
                            .expect("No metadata to clone at post process")
                            .clone();
                        if let Some(cardinality) = eof_metadata.get_mut(DATABLOCK_CARDINALITY) {
                            *cardinality = MetaCell::from(1.0);
                        }
                        let post_process_dblock = DataBlock::new(df_acc, eof_metadata);
                        let post_process_msg = DataMessage::from(post_process_dblock);
                        output_stream.write(post_process_msg)
                    }
                    output_stream.write(message);
                    break;
                }
                Payload::Some(dblock) => {
                    last_metadata = Some(dblock.metadata().clone());
                    if let Some(output_df) = self.process_msg(dblock.data()) {
                        let output_dblock = DataBlock::new(
                            output_df,
                            last_metadata.as_ref().unwrap().clone());
                        let output_message = DataMessage::from(output_dblock);
                        output_stream.write(output_message);
                    }
                }
                Payload::Signal(_) => {
                    break;
                }
            }
            log::info!(
                "[logging] type=execution thread={:?} action=process time={:?}",
                std::thread::current().id(),
                start_time.elapsed().as_micros()
            );
            start_time = std::time::Instant::now();
        }
        log::info!(
            "[logging] type=execution thread={:?} action=process time={:?}",
            std::thread::current().id(),
            start_time.elapsed().as_micros()
        );
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
