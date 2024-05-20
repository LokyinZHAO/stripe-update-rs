use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
};

use crate::storage::BlockId;

use super::RangeSet;

#[derive(Debug)]
pub struct LruEvict {
    lru: RefCell<lru::LruCache<BlockId, RangeSet>>,
    len: Cell<usize>,
    capacity: usize,
}

impl LruEvict {
    pub fn with_capacity(cap: NonZeroUsize) -> Self {
        Self {
            lru: RefCell::new(lru::LruCache::unbounded()),
            len: Cell::new(0),
            capacity: cap.get(),
        }
    }
}

impl super::EvictStrategySlice for LruEvict {
    fn contains(&self, block_id: crate::storage::BlockId) -> bool {
        self.lru.borrow().contains(&block_id)
    }

    fn len(&self) -> usize {
        self.len.get()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn get(&self, block_id: crate::storage::BlockId) -> Option<super::RangeSet> {
        self.lru.borrow().peek(&block_id).cloned()
    }

    fn push(
        &self,
        block_id: crate::storage::BlockId,
        range: std::ops::Range<usize>,
    ) -> Option<(crate::storage::BlockId, super::RangeSet)> {
        let mut lru = self.lru.borrow_mut();
        let mut rangeset = lru.pop(&block_id).unwrap_or_default();
        let inc = rangeset.insert(range);
        lru.put(block_id, rangeset);
        (!inc.is_empty())
            .then(|| {
                let inc_len: usize = inc.iter().map(std::ops::Range::len).sum();
                self.len.set(self.len.get() + inc_len);
                drop(lru);
                // pop if the size exceeds the capacity
                (self.len.get() > self.capacity).then(|| self.pop_first().unwrap())
            })
            .flatten()
    }

    fn pop_first(&self) -> Option<(crate::storage::BlockId, super::RangeSet)> {
        let mut lru = self.lru.borrow_mut();
        let (block_id, rangeset) = lru.pop_lru()?;
        let len = rangeset.len();
        self.len.set(self.len.get() - len);
        Some((block_id, rangeset))
    }

    fn pop_with_id(&self, block_id: crate::storage::BlockId) -> Option<super::RangeSet> {
        let mut lru = self.lru.borrow_mut();
        let rangeset = lru.pop(&block_id)?;
        let len = rangeset.len();
        self.len.set(self.len.get() - len);
        Some(rangeset)
    }
}
