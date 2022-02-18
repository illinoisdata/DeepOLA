#[macro_use]
extern crate generator;

pub mod data;
pub mod processor;
pub mod graph;
pub mod operations;

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
