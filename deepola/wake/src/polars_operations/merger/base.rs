use crate::{graph::ExecutionNode, processor::StreamProcessor};
use getset::{Getters, Setters};
use std::marker::PhantomData;

/// Factory for creating an [ExecutionNode] that merge-joins two frames already sorted
/// on their join keys.
#[derive(Getters, Setters)]
pub struct MergerNode<T, P: MergerOp<T>> {
    #[set = "pub"]
    merger: P,

    // Necessary to have T as a generic type
    phantom: PhantomData<T>,
}

/// Creates a node that simply passes the data coming from the left.
impl<T, P: MergerOp<T> + Clone> Default for MergerNode<T, P> {
    fn default() -> Self {
        Self {
            merger: P::new(),
            phantom: PhantomData::default(),
        }
    }
}

impl<T: 'static + Send, P> MergerNode<T, P>
where
    P: 'static + MergerOp<T> + StreamProcessor<T> + Clone,
{
    pub fn new() -> Self {
        MergerNode::default()
    }

    pub fn merger(&mut self, op: P) -> &mut Self {
        self.merger = op;
        self
    }

    pub fn build(&self) -> ExecutionNode<T> {
        let data_processor = Box::new(self.merger.clone());
        ExecutionNode::<T>::new(data_processor, 2)
    }
}

/// Represents a generic merger operation. For [MergerNode], a concrete struct implementing this
/// trait (e.g., SortedDfMerger) must be used.
pub trait MergerOp<T>: Send {
    fn new() -> Self
    where
        Self: Sized;

    fn supply_left(&self, df_left: &T);

    fn supply_right(&self, df_right: &T);

    fn needs_left(&self) -> bool;

    fn needs_right(&self) -> bool;

    fn merge(&self) -> T;
}
