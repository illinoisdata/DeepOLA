#[macro_use]
extern crate generator;

pub mod channel;
pub mod data;
pub mod graph;
pub mod operations;
pub mod processor;
pub mod benchmarks;
pub mod forecast;

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
