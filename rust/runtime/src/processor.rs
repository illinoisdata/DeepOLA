/// The interface for ExecutionNode.
///
/// This is the interface for processing a set of input dataset and produces
/// a set of output dataset, in each iteration. ExecutioNode repeats such an
/// iteration indefinitely until it sees EOF.
///
/// Since this is a generic trait, concrete implementations must be provided.
/// See [`SimpleMap`] for an example
pub trait SetProcessor<T> : Send {
    fn process(&self, kv_set: &Vec<T>) -> Vec<T>;
}

/// Processes a set of dataset in a stateless manner.
pub struct SimpleMapper<T> {
    record_map: Box<dyn Fn(&T) -> Option<T>>,
}

unsafe impl<T> Send for SimpleMapper<T> {}

impl<T> SetProcessor<T> for SimpleMapper<T> {
    fn process(&self, input_set: &Vec<T>) -> Vec<T> {
        let mut records: Vec<T> = vec![];
        for r in input_set.iter() {
            match (self.record_map)(r) {
                Some(a) => records.push(a),
                None => (),
            }
        }
        records
    }
}

/// Factory methods for [`SimpleMap`]
impl<T> SimpleMapper<T> {
    // This argument type is overly complex, but this is what we need for passing Closure
    // See https://stackoverflow.com/questions/27831944/how-do-i-store-a-closure-in-a-struct-in-rust
    //
    // `'static` means that the passed closure will live until the entire program ends.
    // See https://doc.rust-lang.org/rust-by-example/scope/lifetime/static_lifetime.html#trait-bound
    //
    // Difference between `dyn` and `impl`:
    // See https://www.ncameron.org/blog/dyn-trait-and-impl-trait-in-rust/#:~:text=Unlike%20impl%20Trait%20%2C%20you%20cannot,type%20without%20a%20wrapping%20pointer.&text=This%20is%20not%20a%20generic,types%20within%20the%20trait%20object.
    //
    // How to store closure in a struct:
    // https://stackoverflow.com/questions/27831944/how-do-i-store-a-closure-in-a-struct-in-rust
    pub fn from_lambda(record_map: impl Fn(&T) -> Option<T> + 'static) -> Self {
        SimpleMapper { record_map: Box::new(record_map) }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, rc::Rc};

    use crate::data::kv::{KeyValue};

    use super::*;

    #[test]
    fn test_closure() {
        let kv_set = vec!(KeyValue::from_str("mykey", "hello"));
        let my_name = "illinois";    // this variable is captured
        let mapper = SimpleMapper::from_lambda(|a: &KeyValue| {
            Some(KeyValue::from_string(a.key().into(), a.value().to_string() + " " + my_name))
        });
        let result = mapper.process(&kv_set);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key(), &"mykey".to_string());
        assert_eq!(result[0].value(), &"hello illinois".to_string());
    }

    #[test]
    fn can_pass_rc() {
        let mapper = SimpleMapper::from_lambda(|a: &KeyValue| Some(a.clone()));
        let kv = KeyValue::from_str("mykey", "myvalue");
        let _output = mapper.process(&Rc::new(vec!(kv)));
    }

    #[test]
    fn can_send() {
        let set_processor: Box<dyn SetProcessor<String>> = 
            Box::new(SimpleMapper::<String>::from_lambda(|r| Some(r.clone())));
        thread::spawn( move || {
            // having `drop` prevents warning.
            drop(set_processor);
        } );
    }
}
