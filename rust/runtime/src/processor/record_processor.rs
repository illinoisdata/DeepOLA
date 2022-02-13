use generator::{Generator, Gn};

use crate::data::DataBlock;

use super::set_processor::SetProcessorV1;

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

impl<T: Send> SetProcessorV1<T> for SimpleMapper<T> {
    fn process_v1<'a>(&'a self, input_set: &'a DataBlock<T>) -> Generator<'a, (), DataBlock<T>> {
        Gn::new_scoped(move |mut s| {
            let mut records: Vec<T> = vec![];
            for r in input_set.data().iter() {
                match (*self.record_map)(r) {
                    Some(a) => records.push(a),
                    None => (),
                }
            }
            let message = DataBlock::from_records(records);
            s.yield_(message);
            done!();
        })
    }
}

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
        let in_dblock = DataBlock::from_records(kv_set);
        let out_dblocks = mapper.process_v1(&in_dblock);
        let mut checked = false;
        for out_dblock in out_dblocks {
            let result = out_dblock.data();
            checked = true;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].key(), &"mykey".to_string());
            assert_eq!(result[0].value(), &"hello illinois".to_string());
        }
        assert_eq!(checked, true);
    }

    #[test]
    fn can_pass_rc() {
        let mapper = SimpleMapper::from(|a: &KeyValue| Some(a.clone()));
        let kv = KeyValue::from_str("mykey", "myvalue");
        let rc = Rc::new(DataBlock::from_records(vec![kv]));
        let _output = mapper.process_v1(&rc);
    }

    #[test]
    fn can_send() {
        let set_processor: Box<dyn SetProcessorV1<String>> =
            Box::new(SimpleMapper::<String>::from(|r: &String| Some(r.clone())));
        thread::spawn(move || {
            // having `drop` prevents warning.
            drop(set_processor);
        });
    }
}
