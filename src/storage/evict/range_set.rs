use std::ops::Range;

type Ranges = range_collections::RangeSet2<usize>;

/// [`RangeSet`] represents a set of ordered ranges.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RangeSet {
    ranges: Ranges,
    len: usize,
}

impl Default for RangeSet {
    fn default() -> Self {
        Self {
            ranges: range_collections::RangeSet2::empty(),
            len: Default::default(),
        }
    }
}

impl RangeSet {
    /// Insert a range.
    /// Nothing happens if `range` is a sub-range of existing ranges,
    /// otherwise the difference ranges will be added to the existing ranges.
    ///
    /// # Return
    /// - the incremental range size after the insertion. `0` indicates that the `range` is a sub-range of the existing ranges.
    ///
    /// # Panics
    /// - If the `range` is not bounded.
    pub fn insert(&mut self, range: Range<usize>) -> usize {
        let range: Ranges = range_collections::RangeSet::from(range);
        let diff: Ranges = range.difference(&self.ranges);
        let len_inc = diff.iter().fold(0_usize, |acc, e| {
            use range_collections::range_set::RangeSetRange;
            if let RangeSetRange::Range(range) = e {
                acc + (*range.end - *range.start)
            } else {
                panic!("unbounded range");
            }
        });
        self.len += len_inc;
        self.ranges.union_with(&range);
        len_inc
    }

    /// Get the total length of the existing ranges.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get a vector of existing ranges
    pub fn to_ranges(&self) -> Vec<Range<usize>> {
        self.ranges
            .boundaries()
            .chunks_exact(2)
            .map(|bound| bound[0]..bound[1])
            .collect()
    }
}

#[cfg(test)]
mod test {
    use super::RangeSet;

    #[test]
    fn test_insert() {
        let mut ranges = RangeSet::default();
        ranges.insert(3..10);
        assert_eq!(ranges.len(), 7);
        ranges.insert(5..9);
        assert_eq!(ranges.len(), 7);
        ranges.insert(10..15);
        assert_eq!(ranges.len(), 12);
        ranges.insert(20..25);
        assert_eq!(ranges.len(), 17);
        ranges.insert(0..1);
        assert_eq!(ranges.len(), 18);
        let bounds = ranges.to_ranges();
        assert_eq!(bounds, vec![0..1, 3..15, 20..25]);
    }
}
