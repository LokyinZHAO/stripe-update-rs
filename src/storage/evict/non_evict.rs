use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
};

use crate::storage::BlockId;

use super::{EvictStrategySlice, RangeSet};

#[derive(Debug, Default)]
/// This eviction strategy never evict any item, that is, it has ultimate capacity
pub struct NonEvict {
    map: RefCell<HashMap<BlockId, RangeSet>>,
    cur_len: Cell<usize>,
}

impl EvictStrategySlice for NonEvict {
    fn contains(&self, block_id: crate::storage::BlockId) -> bool {
        self.map.borrow().contains_key(&block_id)
    }

    fn len(&self) -> usize {
        self.cur_len.get()
    }

    fn capacity(&self) -> usize {
        usize::MAX
    }

    fn get(&self, block_id: crate::storage::BlockId) -> Option<RangeSet> {
        self.map.borrow().get(&block_id).map(ToOwned::to_owned)
    }

    fn push(
        &self,
        block_id: crate::storage::BlockId,
        range: std::ops::Range<usize>,
    ) -> Option<(crate::storage::BlockId, RangeSet)> {
        let mut map = self.map.borrow_mut();
        let inc_range = map
            .get_mut(&block_id)
            .map(|exist| exist.insert(range.clone()))
            .unwrap_or_else(|| {
                let mut range_set = RangeSet::default();
                let ret = range_set.insert(range);
                map.insert(block_id, range_set);
                ret
            });
        self.cur_len
            .set(self.cur_len.get() + inc_range.iter().map(std::ops::Range::len).sum::<usize>());
        None
    }

    fn pop_first(&self) -> Option<(crate::storage::BlockId, RangeSet)> {
        let mut map = self.map.borrow_mut();
        map.keys().nth(0).map(ToOwned::to_owned).map(|key| {
            let ret = map.remove_entry(&key).unwrap();
            self.cur_len.set(self.cur_len.get() - ret.1.len());
            ret
        })
    }

    fn pop_with_id(&self, block_id: crate::storage::BlockId) -> Option<RangeSet> {
        self.map
            .borrow_mut()
            .remove(&block_id)
            .inspect(|evict_range| self.cur_len.set(self.cur_len.get() - evict_range.len()))
    }
}
