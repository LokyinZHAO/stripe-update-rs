use std::ops::Range;

use super::BlockId;

mod most_modified;
mod range_set;

pub use most_modified::MostModifiedEvict;
pub use range_set::RangeSet;

pub trait EvictStrategySlice {
    /// Return `true` if the evict contains a block, otherwise `false`.
    fn contains(&self, block_id: BlockId) -> bool;
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
    /// Pop a block with its corresponding ranges.
    ///
    /// # Return
    /// - [`Some`] a block with its corresponding ranges popped by a specific eviction strategy
    /// - [`None`] if empty
    fn pop(&self) -> Option<(BlockId, RangeSet)>;
}
