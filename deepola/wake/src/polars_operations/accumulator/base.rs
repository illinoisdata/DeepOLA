use std::marker::PhantomData;

use getset::{Getters, Setters};

use crate::{graph::ExecutionNode, processor::MessageProcessor};

/// Factory class for creating an ExecutionNode that can perform AccumulatorOp.
#[derive(Getters, Setters)]
pub struct AccumulatorNode<T, P: AccumulatorOp<T>> {
    #[set = "pub"]
    accumulator: P,

    // Necessary to have T as a generic type
    phantom: PhantomData<T>,
}

/// Creates an identity accumulator that passes all the information as it is.
impl<T, P: AccumulatorOp<T> + Clone> Default for AccumulatorNode<T, P> {
    fn default() -> Self {
        Self {
            accumulator: P::new(),
            phantom: PhantomData::default(),
        }
    }
}

impl<T: 'static + Send, P> AccumulatorNode<T, P>
where
    P: 'static + AccumulatorOp<T> + MessageProcessor<T> + Clone,
{
    pub fn new() -> Self {
        AccumulatorNode::default()
    }

    pub fn accumulator(&mut self, op: P) -> &mut Self {
        self.accumulator = op;
        self
    }

    pub fn build(&self) -> ExecutionNode<T> {
        let data_processor = Box::new(self.accumulator.clone());
        ExecutionNode::<T>::new(data_processor, 1)
    }
}

/// The internal accumulation operation performed by [AccumulatorNode].
///
/// This operation type supposed to accumulate the results that have been observed thus far
/// in some way (where the way should be defined by implementing structs).
pub trait AccumulatorOp<T>: Send {
    /// Creates a new struct with an initial state.
    fn new() -> Self;

    /// Given a new dataframe, accumulates it with the other dataframes that have been observed
    /// thus far, and produces a new result. The past observations must be kept in the implementing
    /// struct itself.
    ///
    /// @df A new dataframe.
    ///
    /// @return The accumulation result.
    fn accumulate(&self, df: &T) -> T;
}
