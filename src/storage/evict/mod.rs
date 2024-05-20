use std::ops::Range;

use super::BlockId;

mod lru_evict;
mod most_modified_block;
mod most_modified_stripe;
mod non_evict;
mod range_set;

pub use lru_evict::LruEvict;
pub use most_modified_block::MostModifiedBlockEvict;
pub use most_modified_stripe::MostModifiedStripeEvict;
pub use non_evict::NonEvict;
pub use range_set::RangeSet;

#[allow(unused)]
pub trait EvictStrategy {
    type Item;
    /// Return `true` if the evict contains an element equal to `item`, otherwise false
    fn contains(&self, item: &Self::Item) -> bool;
    /// Push an item into the container.
    /// If the container is full, it returns the evicted item, other wise `None`
    fn push(&self, item: Self::Item) -> Option<Self::Item>;
    /// Pop an item from the container.
    /// If the container is empty, it returns `None`.
    fn pop(&self) -> Option<Self::Item>;
}

pub trait EvictStrategySlice: std::fmt::Debug {
    /// Return `true` if the evict contains a block, otherwise `false`.
    fn contains(&self, block_id: BlockId) -> bool;
    /// Return the current size of the slices stored.
    fn len(&self) -> usize;
    /// Return `true` if there is no slice stored.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Return the maximum slice size can store before eviction.
    fn capacity(&self) -> usize;
    /// Get the slice ranges corresponding to the block.
    ///
    /// # Returns
    /// - [`Some`] if the block exists
    /// - [`None`] if the block does not exist
    fn get(&self, block_id: BlockId) -> Option<RangeSet>;
    /// Push a slice range to a block.
    /// If the block already exists, the corresponding slice range will be merged and updated.
    /// If the block does not exist, a new entry will be inserted.
    /// This may cause a eviction, and the evicted entry with [`BlockId`] and corresponding ranges will be returned.
    ///
    /// # Parameters
    /// - `block_id`: the id of the block to update
    /// - `range`: a new range to push
    ///
    /// # Return
    /// - [`Some`] if a block with its range was evicted.
    /// - [`None`] if no eviction happens
    fn push(&self, block_id: BlockId, range: Range<usize>) -> Option<(BlockId, RangeSet)>;
    /// Pop the first block with its corresponding ranges according to the evict strategy.
    ///
    /// # Return
    /// - [`Some`] a block with its corresponding ranges popped by a specific eviction strategy
    /// - [`None`] if empty
    fn pop_first(&self) -> Option<(BlockId, RangeSet)>;

    /// Pop the block with its corresponding ranges by `block_id`
    ///
    /// # Return
    /// -[`Some`] ranges previously pushed if the block exits
    /// -[`None`] if the block does not exit
    fn pop_with_id(&self, block_id: BlockId) -> Option<RangeSet>;
}
