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
    /// The incremental ranges after the insertion.
    /// The ranges may be empty if there is no incremental range inserted into the existing ranges.
    ///
    /// # Panics
    /// - If the `range` is not bounded.
    pub fn insert(&mut self, range: Range<usize>) -> smallvec::SmallVec<[Range<usize>; 1]> {
        let range: Ranges = range_collections::RangeSet::from(range);
        let diff: Ranges = range.difference(&self.ranges);
        let inc_ranges: smallvec::SmallVec<[Range<usize>; 1]> = diff
            .iter()
            .map(|range| {
                use range_collections::range_set::RangeSetRange;
                if let RangeSetRange::Range(range) = range {
                    *range.start..*range.end
                } else {
                    panic!("unbounded range")
                }
            })
            .collect();
        let len_inc: usize = inc_ranges.iter().map(Range::len).sum();
        self.len += len_inc;
        self.ranges.union_with(&range);
        inc_ranges
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

    pub fn into_inner(self) -> Ranges {
        self.ranges
    }
}

impl std::ops::Deref for RangeSet {
    type Target = Ranges;

    fn deref(&self) -> &Self::Target {
        &self.ranges
    }
}

impl From<&[Range<usize>]> for RangeSet {
    fn from(value: &[Range<usize>]) -> Self {
        let mut range_set = RangeSet::default();
        value.iter().for_each(|range| {
            let _ = range_set.insert(range.to_owned());
        });
        range_set
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use super::RangeSet;

    #[test]
    fn test_insert() {
        use smallvec::*;
        let mut ranges = RangeSet::default();

        let inc = ranges.insert(3..10);
        let expect: SmallVec<[Range<usize>; 1]> = smallvec![3..10];
        assert_eq!(inc, expect);
        assert_eq!(ranges.len(), 7);

        let inc = ranges.insert(5..9);
        let expect: SmallVec<[Range<usize>; 1]> = smallvec![];
        assert_eq!(inc, expect);
        assert_eq!(ranges.len(), 7);

        let inc = ranges.insert(10..15);
        let expect: SmallVec<[Range<usize>; 1]> = smallvec![10..15];
        assert_eq!(inc, expect);
        assert_eq!(ranges.len(), 12);

        let inc = ranges.insert(20..25);
        let expect: SmallVec<[Range<usize>; 1]> = smallvec![20..25];
        assert_eq!(inc, expect);
        assert_eq!(ranges.len(), 17);

        let inc = ranges.insert(0..1);
        let expect: SmallVec<[Range<usize>; 1]> = smallvec![0..1];
        assert_eq!(inc, expect);
        assert_eq!(ranges.len(), 18);

        let inc = ranges.insert(2..23);
        let expect: SmallVec<[Range<usize>; 1]> = smallvec![2..3, 15..20];
        assert_eq!(inc, expect);
        assert_eq!(ranges.len(), 24);

        let bounds = ranges.to_ranges();
        assert_eq!(bounds, vec![0..1, 2..25]);
    }
}
