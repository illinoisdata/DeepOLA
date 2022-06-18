use crate::data::DataBlock;

use super::set_processor::SetProcessorV2;

/// Processes a set of dataset in a stateless manner.
pub struct SimpleMapper<T> {
    record_map: Box<dyn Fn(&T) -> Option<T>>,
}

impl<T: Send, F> From<F> for SimpleMapper<T>
where
    F: Fn(&T) -> Option<T> + 'static,
{
    /// Constructor from a closure
    fn from(record_map: F) -> Self {
        SimpleMapper {
            record_map: Box::new(record_map),
        }
    }
}

unsafe impl<T> Send for SimpleMapper<T> {}

impl<T: Send> SetProcessorV2<T> for SimpleMapper<T> {
    fn process_v1(&self, input_set: &DataBlock<T>) -> DataBlock<T> {
        let mut records: Vec<T> = vec![];
        for r in input_set.data().iter() {
            if let Some(a) = (*self.record_map)(r) { records.push(a) }
        }
        DataBlock::from(records)
    }
}

// impl<T: Send> SetProcessorV1<T> for SimpleMapper<T> {
//     fn process_v1<'a>(&'a self, input_set: &'a DataBlock<T>) -> Generator<'a, (), DataBlock<T>> {
//         Gn::new_scoped(move |mut s| {
//             let mut records: Vec<T> = vec![];
//             for r in input_set.data().iter() {
//                 if let Some(a) = (*self.record_map)(r) { records.push(a) }
//             }
//             let message = DataBlock::from(records);
//             s.yield_(message);
//             done!();
//         })
//     }
// }

#[cfg(test)]
mod tests {
    use std::{rc::Rc, thread};

    use crate::data::KeyValue;

    use super::*;

    #[test]
    fn test_closure() {
        let kv_set = vec![KeyValue::from_str("mykey", "hello")];
        let my_name = "illinois"; // this variable is captured
        let mapper = SimpleMapper::from(|a: &KeyValue| {
            Some(KeyValue::from_string(
                a.key().into(),
                a.value().to_string() + " " + my_name,
            ))
        });
        let in_dblock = DataBlock::from(kv_set);
        let out_dblock = mapper.process_v1(&in_dblock);
        let result = out_dblock.data();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key(), &"mykey".to_string());
        assert_eq!(result[0].value(), &"hello illinois".to_string());
    }

    #[test]
    fn can_pass_rc() {
        let mapper = SimpleMapper::from(|a: &KeyValue| Some(a.clone()));
        let kv = KeyValue::from_str("mykey", "myvalue");
        let rc = Rc::new(DataBlock::from(vec![kv]));
        let _output = mapper.process_v1(&rc);
    }

    #[test]
    fn can_send() {
        let set_processor: Box<dyn SetProcessorV2<String>> =
            Box::new(SimpleMapper::<String>::from(|r: &String| Some(r.clone())));
        thread::spawn(move || {
            // having `drop` prevents warning.
            drop(set_processor);
        });
    }
}
