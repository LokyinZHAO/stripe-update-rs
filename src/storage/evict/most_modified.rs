use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
};

use crate::storage::BlockId;

use super::{EvictStrategySlice, RangeSet};

/// Wrapper for [`RangeSet`], whose order is compared by len()
#[derive(Debug, Default, Eq, Clone)]
struct RangeSetCmpByLen(RangeSet);

impl std::cmp::PartialEq for RangeSetCmpByLen {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
    }
}

impl std::cmp::PartialOrd for RangeSetCmpByLen {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for RangeSetCmpByLen {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.len().cmp(&other.0.len())
    }
}

type InnerQueue = RefCell<priority_queue::PriorityQueue<BlockId, RangeSetCmpByLen>>;

/// A container with block and its ranges as entries.
/// This eviction strategy record the slice range size of a block, and maintain a maximum size.
/// If current size exceeds the maximum size, a block with the max slice size will be evicted.
///
/// This can be used as the most modified eviction strategy.
pub struct MostModifiedEvict {
    queue: InnerQueue,
    max_size: usize,
    cur_size: Cell<usize>,
}

impl MostModifiedEvict {
    /// Make a [`MostModifiedEvict`] instance.
    ///
    /// # Parameter
    /// - `max_size`: max slice size this instance can maintain.
    pub fn with_max_size(max_size: NonZeroUsize) -> Self {
        let max_size = max_size.get();
        Self {
            max_size,
            queue: Default::default(),
            cur_size: Cell::new(0),
        }
    }
}

impl EvictStrategySlice for MostModifiedEvict {
    /// Return `true` if the evict contains a block, otherwise `false`.
    fn contains(&self, block_id: crate::storage::BlockId) -> bool {
        self.queue.borrow().get_priority(&block_id).is_some()
    }

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
    fn push(
        &self,
        block_id: crate::storage::BlockId,
        range: std::ops::Range<usize>,
    ) -> Option<(crate::storage::BlockId, super::RangeSet)> {
        let mut queue = self.queue.borrow_mut();
        let mut new_range = queue
            .get_priority(&block_id)
            .map(|ranges| ranges.to_owned())
            .unwrap_or_default();
        let inc_size = new_range.0.insert(range);
        (inc_size > 0)
            .then(|| {
                queue.push(block_id, new_range);
                self.cur_size.set(self.cur_size.get() + inc_size);
                (self.cur_size.get() > self.max_size).then(|| {
                    // evict
                    let (evict_block_id, evict_ranges) = queue.pop().unwrap();
                    self.cur_size
                        .set(self.cur_size.get() - evict_ranges.0.len());
                    (evict_block_id, evict_ranges.0)
                })
            })
            .flatten()
    }

    /// Pop a block with its corresponding ranges.
    ///
    /// # Return
    /// - [`Some`] a block with its corresponding ranges popped by a specific eviction strategy
    /// - [`None`] if empty
    fn pop(&self) -> Option<(crate::storage::BlockId, super::RangeSet)> {
        self.queue
            .borrow_mut()
            .pop()
            .map(|(block_id, ranges)| (block_id, ranges.0))
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use crate::storage::evict::{most_modified::MostModifiedEvict, EvictStrategySlice};

    #[test]
    fn test_evict() {
        const MAX_SIZE: usize = 40;
        let mm = MostModifiedEvict::with_max_size(NonZeroUsize::new(MAX_SIZE).unwrap());
        assert!(mm.push(1, 5..20).is_none()); // [1: 5..20]
        assert!(mm.push(1, 0..10).is_none()); // [1: 0..20]
        assert_eq!(mm.cur_size.get(), 20);
        assert!(mm.push(2, 20..30).is_none()); // [1: 0..20], [2: 20..30]
        let evict = mm.push(2, 50..70).unwrap(); // [1: 0..20]
        assert_eq!(evict.0, 2);
        assert_eq!(evict.1.to_ranges(), vec![20..30, 50..70]);
        assert!(mm.push(1, 20..30).is_none()); // [1: 0..30]
        let evict = mm.push(3, 0..20).unwrap(); // [3: 0..20]
        assert_eq!(evict.0, 1);
        assert_eq!(evict.1.to_ranges(), vec![0..30]);
        assert!(mm.push(3, 30..50).is_none()); // [3: 0..20, 30..50]
        let evict = mm.push(3, 25..26).unwrap(); // empty
        assert_eq!(evict.0, 3);
        assert_eq!(evict.1.to_ranges(), vec![0..20, 25..26, 30..50]);
        assert!(mm.pop().is_none());
    }
}
