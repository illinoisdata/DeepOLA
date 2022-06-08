#[cfg(test)]
mod tests {

    use std::sync::{mpsc::channel, Arc};
    use std::thread;

    use polars::prelude::{NamedFrom, Series};

    use crate::data::DataBlock;

    #[test]
    fn test() {
        // Create a simple streaming channel
        let (tx, rx) = channel();
        let s = DataBlock::from(vec![Series::new(
            "boolean series",
            &vec![true, false, true],
        )]);

        thread::spawn(move || {
            tx.send(s).unwrap();
        });
        assert_eq!(
            rx.recv().unwrap(),
            DataBlock::from(vec![Series::new(
                "boolean series",
                &vec![true, false, true]
            )])
        );
    }
}
