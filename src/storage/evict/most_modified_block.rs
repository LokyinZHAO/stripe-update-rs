use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
    ops::Range,
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
#[derive(Debug)]
pub struct MostModifiedBlockEvict {
    queue: InnerQueue,
    max_size: usize,
    cur_size: Cell<usize>,
}

impl MostModifiedBlockEvict {
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

impl EvictStrategySlice for MostModifiedBlockEvict {
    /// Return `true` if the evict contains a block, otherwise `false`.
    fn contains(&self, block_id: crate::storage::BlockId) -> bool {
        self.queue.borrow().get_priority(&block_id).is_some()
    }

    /// Return the current size of the slices stored.
    fn len(&self) -> usize {
        self.cur_size.get()
    }

    /// Return the maximum slice size can store before eviction.
    fn capacity(&self) -> usize {
        self.max_size
    }

    /// Get the slice ranges corresponding to the block.
    ///
    /// # Returns
    /// - [`Some`] with the modified ranges if the block exists
    /// - [`None`] if the block does not exist
    fn get(&self, block_id: BlockId) -> Option<RangeSet> {
        self.queue
            .borrow()
            .get_priority(&block_id)
            .map(|ranges| ranges.0.clone())
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
        let inc_ranges = if queue.get_priority(&block_id).is_some() {
            let mut inc_range_opt = None::<smallvec::SmallVec<[Range<usize>; 1]>>;
            let ret = queue.change_priority_by(&block_id, |range_set| {
                let inc_range = range_set.0.insert(range);
                inc_range_opt = Some(inc_range)
            });
            assert!(ret);
            inc_range_opt.unwrap()
        } else {
            let mut range_set = RangeSet::default();
            let inc_range = range_set.insert(range.clone());
            let ret = queue.push(block_id, RangeSetCmpByLen(range_set));
            debug_assert!(ret.is_none());
            inc_range
        };
        (!inc_ranges.is_empty())
            .then(|| {
                let inc_size: usize = inc_ranges.iter().map(std::ops::Range::len).sum();
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

    /// Pop the first block with its corresponding ranges according to the evict strategy.
    ///
    /// # Return
    /// - [`Some`] a block with its corresponding ranges popped by a specific eviction strategy
    /// - [`None`] if empty
    fn pop_first(&self) -> Option<(crate::storage::BlockId, super::RangeSet)> {
        self.queue.borrow_mut().pop().map(|(block_id, ranges)| {
            self.cur_size.set(self.cur_size.get() - ranges.0.len());
            (block_id, ranges.0)
        })
    }

    /// Pop the block with its corresponding ranges by `block_id`
    ///
    /// # Return
    /// -[`Some`] ranges previously pushed if the block exits
    /// -[`None`] if the block does not exit
    fn pop_with_id(&self, block_id: BlockId) -> Option<RangeSet> {
        self.queue
            .borrow_mut()
            .remove(&block_id)
            .map(|(_, ranges)| {
                self.cur_size.set(self.cur_size.get() - ranges.0.len());
                ranges.0
            })
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use crate::storage::evict::{most_modified_block::MostModifiedBlockEvict, EvictStrategySlice};

    #[test]
    fn test_evict() {
        const MAX_SIZE: usize = 40;
        let mm = MostModifiedBlockEvict::with_max_size(NonZeroUsize::new(MAX_SIZE).unwrap());
        assert!(mm.push(1, 5..20).is_none()); // [1: 5..20]
        assert!(mm.push(1, 0..10).is_none()); // [1: 0..20]
        assert_eq!(mm.cur_size.get(), 20);
        assert!(mm.push(2, 20..30).is_none()); // [1: 0..20], [2: 20..30]
        assert!(mm.push(3, 30..40).is_none()); // [1: 0..20], [2: 20..30] [3: 30..40]
        let evict = mm.pop_with_id(3).unwrap();
        assert_eq!(evict.to_ranges(), vec![30..40]);
        let evict = mm.push(2, 50..70).unwrap(); // [1: 0..20]
        assert_eq!(evict.0, 2);
        assert_eq!(evict.1.to_ranges(), vec![20..30, 50..70]);
        assert!(mm.push(1, 20..30).is_none()); // [1: 0..30]
        let evict = mm.push(3, 0..20).unwrap(); // [3: 0..20]
        assert_eq!(evict.0, 1);
        assert_eq!(evict.1.to_ranges(), vec![0..30]);
        assert!(mm.push(3, 30..50).is_none()); // [3: 0..20, 30..50]
        let evict = mm.pop_first().unwrap(); // empty
        assert_eq!(evict.0, 3);
        assert_eq!(evict.1.to_ranges(), vec![0..20, 30..50]);
        assert!(mm.pop_first().is_none());
    }
}
