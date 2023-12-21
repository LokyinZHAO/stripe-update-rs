use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
    ops::Range,
};

use crate::storage::{BlockId, StripeId};

use super::{EvictStrategySlice, RangeSet};

type InnerStripeIdx = usize;

#[derive(Debug, Eq)]
struct StripeRangeSet {
    len: usize,
    range_vec: Vec<RangeSet>,
}

impl StripeRangeSet {
    fn with_m(m: usize) -> Self {
        Self {
            len: 0,
            range_vec: vec![RangeSet::default(); m],
        }
    }

    fn get_at(&self, idx: InnerStripeIdx) -> &RangeSet {
        &self.range_vec[idx]
    }

    fn insert_at(
        &mut self,
        idx: InnerStripeIdx,
        range: Range<usize>,
    ) -> smallvec::SmallVec<[Range<usize>; 1]> {
        let inc = (self.range_vec)[idx].insert(range);
        self.len += inc.iter().map(std::ops::Range::len).sum::<usize>();
        inc
    }

    fn take_at(&mut self, idx: InnerStripeIdx) -> RangeSet {
        let take = std::mem::take(&mut self.range_vec[idx]);
        self.len -= take.len();
        take
    }
}

impl PartialEq for StripeRangeSet {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
    }
}

impl Ord for StripeRangeSet {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.len.cmp(&other.len)
    }
}

impl PartialOrd for StripeRangeSet {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

type InnerQueue = RefCell<priority_queue::PriorityQueue<StripeId, StripeRangeSet>>;

#[derive(Debug)]
pub struct MostModifiedStripeEvict {
    stripe_m: usize,
    max_size: usize,
    queue: InnerQueue,
    cur_size: Cell<usize>,
}

impl MostModifiedStripeEvict {
    pub fn new(stripe_m: NonZeroUsize, max_size: NonZeroUsize) -> Self {
        MostModifiedStripeEvict {
            stripe_m: stripe_m.get(),
            max_size: max_size.get(),
            queue: RefCell::new(priority_queue::PriorityQueue::with_capacity(64)),
            cur_size: Cell::new(0),
        }
    }
    fn block_id_to_stripe_idx(&self, block_id: BlockId) -> (StripeId, InnerStripeIdx) {
        ((block_id / self.stripe_m).into(), block_id % self.stripe_m)
    }

    fn stripe_idx_to_block_to_id(&self, stripe_id: StripeId, idx: InnerStripeIdx) -> BlockId {
        stripe_id.0 * self.stripe_m + idx
    }
}

impl EvictStrategySlice for MostModifiedStripeEvict {
    fn contains(&self, block_id: crate::storage::BlockId) -> bool {
        let (stripe_id, idx) = self.block_id_to_stripe_idx(block_id);
        self.queue
            .borrow()
            .get(&stripe_id)
            .map(|(_, ranges)| !ranges.get_at(idx).is_empty())
            .unwrap_or(false)
    }

    fn len(&self) -> usize {
        self.cur_size.get()
    }

    fn capacity(&self) -> usize {
        self.max_size
    }

    fn get(&self, block_id: crate::storage::BlockId) -> Option<super::RangeSet> {
        let (stripe_id, idx) = self.block_id_to_stripe_idx(block_id);
        self.queue
            .borrow()
            .get(&stripe_id)
            .map(|(_, ranges)| ranges.get_at(idx).clone())
    }

    fn push(
        &self,
        block_id: crate::storage::BlockId,
        range: std::ops::Range<usize>,
    ) -> Option<(crate::storage::BlockId, super::RangeSet)> {
        let (stripe_id, idx) = self.block_id_to_stripe_idx(block_id);
        let mut queue = self.queue.borrow_mut();
        if queue.get_priority(&stripe_id).is_none() {
            let ret = queue.push(stripe_id, StripeRangeSet::with_m(self.stripe_m));
            debug_assert!(ret.is_none());
        }
        let mut inc_range_opt = None::<smallvec::SmallVec<[Range<usize>; 1]>>;
        let ret = queue.change_priority_by(&stripe_id, |stripe_ranges| {
            let inc_range = stripe_ranges.insert_at(idx, range);
            inc_range_opt = Some(inc_range);
        });
        assert!(ret);
        let inc_range = inc_range_opt.unwrap();
        (!inc_range.is_empty())
            .then(|| {
                let inc_size: usize = inc_range.iter().map(std::ops::Range::len).sum();
                self.cur_size.set(self.cur_size.get() + inc_size);
                drop(queue);
                (self.cur_size.get() > self.max_size).then(|| self.pop_first().unwrap())
            })
            .flatten()
    }

    fn pop_first(&self) -> Option<(crate::storage::BlockId, super::RangeSet)> {
        // evict
        let queue = self.queue.borrow();
        queue
            .peek()
            .map(|(&evict_stripe_id, _)| evict_stripe_id)
            .map(|evict_stripe_id| {
                let max_len_block_idx = queue
                    .get_priority(&evict_stripe_id)
                    .unwrap()
                    .range_vec
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, item)| item.len())
                    .map(|(idx, _)| idx)
                    .unwrap();
                let block_id = self.stripe_idx_to_block_to_id(evict_stripe_id, max_len_block_idx);
                drop(queue);
                let range_set = self.pop_with_id(block_id).unwrap();
                (block_id, range_set)
            })
    }

    fn pop_with_id(&self, block_id: crate::storage::BlockId) -> Option<super::RangeSet> {
        // evict
        let mut queue = self.queue.borrow_mut();
        let (stripe_id, block_idx) = self.block_id_to_stripe_idx(block_id);
        let mut range_opt = None::<RangeSet>;
        let mut empty_stripe = false;
        let _ret = queue.change_priority_by(&stripe_id, |stripe_ranges| {
            let range = stripe_ranges.take_at(block_idx);
            let range_size = range.len();
            self.cur_size.set(self.cur_size.get() - range_size);
            empty_stripe = stripe_ranges.len == 0;
            if !range.is_empty() {
                range_opt = Some(range);
            }
        });
        if empty_stripe {
            let _ = queue.remove(&stripe_id).unwrap();
        }
        range_opt
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use crate::storage::{
        evict::most_modified_stripe::MostModifiedStripeEvict, EvictStrategySlice,
    };

    #[test]
    fn test_most_modified_stripe() {
        const MAX_SIZE: usize = 100;
        const EC_M: usize = 4;
        let mms = MostModifiedStripeEvict::new(
            NonZeroUsize::new(EC_M).unwrap(),
            NonZeroUsize::new(MAX_SIZE).unwrap(),
        );
        let evict = mms.push(1, 0..20); // 20: (1: [0..20])
        assert!(evict.is_none());
        assert_eq!(mms.len(), 20);
        let evict = mms.push(3, 30..50); // 40: (1: [0..20], 3: [30..50])
        assert!(evict.is_none());
        assert_eq!(mms.len(), 40);
        let evict = mms.push(6, 20..50); // 70: (1: [0..20], 3: [30..50]), (6: [20..50])
        assert!(evict.is_none());
        assert_eq!(mms.len(), 70);
        let evict = mms.push(6, 40..70); // 90: (1: [0..20], 3: [30..50]), (6: [20..70])
        assert!(evict.is_none());
        assert_eq!(mms.len(), 90);
        let evict = mms.push(3, 10..20); // 100: (1: [0..20], 3: [10..20, 30..50]), (6: [20..70])
        assert!(evict.is_none());
        assert_eq!(mms.len(), 100);
        let evict = mms.push(6, 5..15).unwrap(); // 50: (1: [0..20], 3: [10..20, 30..50])
        assert_eq!(mms.len(), 50);
        assert_eq!(evict.0, 6);
        assert_eq!(evict.1.to_ranges(), vec![5..15, 20..70]);
        let evict = mms.push(3, 10..30); // 60: (1: [0..20], 3: [10..50])
        assert!(evict.is_none());
        assert_eq!(mms.len(), 60);
        let evict = mms.push(6, 0..10); // 70: (1: [0..20], 3: [10..50]), (6: [0..10])
        assert_eq!(mms.len(), 70);
        assert!(evict.is_none());
        let evict = mms.push(4, 90..120); // 100: (1: [0..20], 3: [10..50]), (4:[90..120], 6: [0..10])
        assert!(evict.is_none());
        let evict = mms.pop_first().unwrap(); // 60: (1: [0..20]), (4: [90..120], 6: [0..10])
        assert_eq!(mms.len(), 60);
        assert_eq!(evict.0, 3);
        assert_eq!(evict.1.to_ranges(), vec![10..50]);
        let evict = mms.pop_with_id(4).unwrap();
        assert_eq!(mms.len(), 30); // 30: (1: [0..20]), (6: [0..10])
        assert_eq!(evict.to_ranges(), vec![90..120]);
        let evict = mms.pop_first().unwrap();
        assert_eq!(evict.0, 1);
        assert_eq!(evict.1.to_ranges(), vec![0..20]);
        let evict = mms.pop_with_id(3);
        assert!(evict.is_none());
        let evict = mms.pop_first().unwrap();
        assert!(mms.is_empty());
        assert_eq!(evict.0, 6);
        assert_eq!(evict.1.to_ranges(), vec![0..10]);
        let evict = mms.pop_first();
        assert!(evict.is_none());
        assert!(mms.is_empty());
    }
}
