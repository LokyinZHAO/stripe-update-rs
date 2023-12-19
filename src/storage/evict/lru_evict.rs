use std::{cell::RefCell, num::NonZeroUsize};

use super::EvictStrategy;

pub struct LruEvict<T> {
    lru: RefCell<lru::LruCache<T, ()>>,
}

impl<T> LruEvict<T>
where
    T: std::hash::Hash + std::cmp::Eq,
{
    pub fn with_capacity(cap: NonZeroUsize) -> Self {
        let lru = RefCell::new(lru::LruCache::new(cap));
        Self { lru }
    }
}

impl<T> EvictStrategy for LruEvict<T>
where
    T: std::hash::Hash + std::cmp::Eq + Clone,
{
    type Item = T;

    /// Return `true` if the evict contains an element equal to `item`, otherwise false
    ///
    /// # Note
    /// This method will **update** the lru list.
    fn contains(&self, item: &Self::Item) -> bool {
        self.lru.borrow_mut().get(item).is_some()
    }
    /// Push an item into the container.
    /// If the container is full, it returns the evicted item, other wise `None`
    ///
    /// # Note
    /// This method will **update** the lru list.
    fn push(&self, item: T) -> Option<T> {
        let mut lru = self.lru.borrow_mut();
        if lru.get(&item).is_some() {
            // item exist
            None
        } else {
            // item does not exist
            lru.push(item, ()).map(|(evict, ())| evict)
        }
    }

    /// Pop the least used item from the container.
    /// If the container is empty, it returns `None`.
    fn pop(&self) -> Option<Self::Item> {
        self.lru.borrow_mut().pop_lru().map(|entry| entry.0)
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use crate::storage::evict::EvictStrategy;

    use super::LruEvict;

    #[test]
    fn general_test() {
        let lru: LruEvict<String> = LruEvict::with_capacity(NonZeroUsize::new(3).unwrap());
        let s1: String = String::from("1");
        let s2: String = String::from("2");
        let s3: String = String::from("3");
        let s4: String = String::from("4");
        assert!(!lru.contains(&s1));
        assert!(lru.push(s1.clone()).is_none()); // 1
        assert!(lru.push(s2.clone()).is_none()); // 2, 1
        assert!(lru.push(s3.clone()).is_none()); // 3, 2, 1
        assert!(lru.push(s2.clone()).is_none()); // 2, 3, 1
        assert!(lru.contains(&s1)); // 1, 2, 3
        assert_eq!(lru.push(s4.clone()), Some(s3.clone())); // 4, 1, 2
        assert!(!lru.contains(&s3)); // 4, 1, 2
        assert!(lru.push(s2.clone()).is_none()); // 2, 4, 1
        assert_eq!(lru.push(s3.clone()), Some(s1.clone())); // 3, 2, 4
        assert_eq!(lru.pop(), Some(s4.clone())); // 3, 2
        assert_eq!(lru.pop(), Some(s2.clone())); // 3
        assert_eq!(lru.pop(), Some(s3.clone()));
        assert_eq!(lru.pop(), None);
    }
}
