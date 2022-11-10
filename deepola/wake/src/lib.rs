pub mod channel;
pub mod data;
pub mod graph;
pub mod inference;
pub mod polars_operations;
pub mod processor;
pub mod forecast;
pub mod utils;

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
