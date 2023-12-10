use std::num::NonZeroUsize;

use bytes::{BufMut, BytesMut};

use crate::SUError;

use super::Block;

/// A [`Stripe`] is composed of `k` source [`Block`]s and `p` parity [`Block`]s,
/// and all the blocks in a stripe are guaranteed to be consistent.
/// Typically a stripe can tolerant at most `p` block faults,
/// and recover the corrupted blocks via [`ErasureCode`](super::ErasureCode).
#[derive(Debug, PartialEq, Eq)]
pub struct Stripe {
    stripe: Vec<Block>,
    k: u8,
    p: u8,
}

impl Stripe {
    #[inline]
    /// number of the source blocks
    pub fn k(&self) -> usize {
        self.k.try_into().unwrap()
    }

    /// number of the parity blocks
    #[inline]
    pub fn p(&self) -> usize {
        self.p.try_into().unwrap()
    }

    /// number of the source and parity blocks
    #[inline]
    pub fn m(&self) -> usize {
        self.k() + self.p()
    }

    /// Get size of the block in the stripe
    #[inline]
    pub fn block_size(&self) -> usize {
        self.stripe.first().unwrap().block_size()
    }

    /// Make a stripe from a vector of blocks, which contains `k` source blocks ans `p` parity blocks.
    ///
    /// # Panics
    /// - If `vec.len() != k + p`
    pub fn from_vec(vec: Vec<Block>, k: NonZeroUsize, p: NonZeroUsize) -> Self {
        let k = k.get();
        let p = p.get();
        assert_eq!(vec.len(), k + p);
        let block_size = vec.first().unwrap().block_size();
        assert!(vec.iter().all(|block| block.block_size() == block_size));
        Self {
            k: k.try_into().unwrap(),
            p: p.try_into().unwrap(),
            stripe: vec,
        }
    }

    /// Make a stripe with `k` source blocks and `p` parity blocks,
    /// and the payload of all the blocks are filled with `0`.
    pub fn zero(k: NonZeroUsize, p: NonZeroUsize, block_size: NonZeroUsize) -> Self {
        let k = k.get();
        let p = p.get();
        let block_size = block_size.get();
        let mut buf = BytesMut::zeroed(block_size * (k + p));
        Self {
            k: k.try_into().unwrap(),
            p: p.try_into().unwrap(),
            stripe: (0..k + p)
                .map(|_| buf.split_to(block_size))
                .map(Block::from)
                .collect(),
        }
    }

    /// Split a stripe to slices of source blocks and parity blocks
    ///
    /// # Return
    /// A tuple whose the first element is a slice of its source blocks,
    /// and the second element is a slice of its parity blocks.
    pub fn split_source_parity(&self) -> (&[Block], &[Block]) {
        self.stripe.split_at(self.k())
    }

    /// Split a stripe to mutable slices of source blocks and parity blocks
    ///
    /// # Return
    /// A tuple whose the first element is a slice of its source blocks,
    /// and the second element is a slice of its parity blocks.
    pub fn split_mut_source_parity(&mut self) -> (&mut [Block], &mut [Block]) {
        let k = self.k();
        self.stripe.split_at_mut(k)
    }

    /// Return a slice of source blocks.
    pub fn as_source(&self) -> &[Block] {
        let k = self.k();
        &self.stripe[0..k]
    }

    /// Return a mutable slice of source blocks.
    pub fn as_mut_source(&mut self) -> &mut [Block] {
        let k = self.k();
        &mut self.stripe[0..k]
    }

    /// Return a slice of parity blocks.
    pub fn as_parity(&self) -> &[Block] {
        let k = self.k();
        let m = self.m();
        &self.stripe[k..m]
    }

    /// Return a mutable slice of parity blocks.
    pub fn as_mut_parity(&mut self) -> &mut [Block] {
        let k = self.k();
        let m = self.m();
        &mut self.stripe[k..m]
    }

    /// Return an iterator over source blocks.
    pub fn iter_source(&self) -> impl ExactSizeIterator<Item = &Block> {
        let k = self.k();
        self.stripe[0..k].iter()
    }

    /// Return a mutable iterator over
    pub fn iter_mut_source(&mut self) -> impl ExactSizeIterator<Item = &mut Block> {
        let k = self.k();
        self.stripe[0..k].iter_mut()
    }

    /// Return an iterator over parity blocks.
    pub fn iter_parity(&self) -> impl ExactSizeIterator<Item = &Block> {
        let k = self.k();
        let m = self.m();
        self.stripe[k..m].iter()
    }

    /// Return a mutable iterator over parity blocks.
    pub fn iter_mut_parity(&mut self) -> impl ExactSizeIterator<Item = &mut Block> {
        let k = self.k();
        let m = self.m();
        self.stripe[k..m].iter_mut()
    }
}

impl Clone for Stripe {
    fn clone(&self) -> Self {
        let mut buf = BytesMut::with_capacity(self.m() * self.block_size());
        let block_size = self.block_size();
        // make a slice from a continuous memory region
        let stripe = self
            .stripe
            .iter()
            .map(|block| {
                buf.put_slice(block);
                buf.split_to(block_size)
            })
            .map(Block::from)
            .collect::<Vec<_>>();
        Self {
            k: self.k,
            p: self.p,
            stripe,
        }
    }
}

impl TryFrom<PartialStripe> for Stripe {
    type Error = SUError;

    /// Try to convert a [`PartialStripe`] to a [`Stripe`].
    ///
    /// # Return
    /// - [`Ok`] if success
    /// - [`Err(SUError::ErasureCode)`] if not all the blocks in the `partial_stripe` is present
    fn try_from(partial_stripe: PartialStripe) -> Result<Self, Self::Error> {
        if !partial_stripe.is_all_present() {
            return Err(Self::Error::erasure_code(
                (file!(), line!(), column!()),
                "not all the blocks are present",
            ));
        }
        let k = partial_stripe.k;
        let p = partial_stripe.p;
        let stripe = partial_stripe
            .stripe
            .into_iter()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        Ok(Self { k, p, stripe })
    }
}

/// A [`PartialStripe`] represents a stripe with some blocks are absent(may be corrupted or not necessarily needed).
/// A present block is represented as [`Some(Block)`],
/// while an absent blocks is represented as [`None`].
/// Like [`Stripe`], the size of all the present blocks are guaranteed to be consistent.
#[derive(Debug, PartialEq, Eq)]
pub struct PartialStripe {
    block_size: usize,
    stripe: Vec<Option<Block>>,
    k: u8,
    p: u8,
}

type PresentHalf<'a> = Vec<(usize, &'a Option<Block>)>;
type AbsentHalf<'a> = Vec<(usize, &'a Option<Block>)>;
type PresentHalfMut<'a> = Vec<(usize, &'a mut Option<Block>)>;
type AbsentHalfMut<'a> = Vec<(usize, &'a mut Option<Block>)>;

impl PartialStripe {
    /// number of the source blocks
    #[inline]
    pub fn k(&self) -> usize {
        self.k.try_into().unwrap()
    }

    /// number of the parity blocks
    #[inline]
    pub fn p(&self) -> usize {
        self.p.try_into().unwrap()
    }

    /// number of the source and parity blocks
    #[inline]
    pub fn m(&self) -> usize {
        self.k() + self.p()
    }

    /// size of a block
    #[inline]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Return `true` if all the blocks are present, otherwise `false`.
    pub fn is_all_present(&self) -> bool {
        self.stripe.iter().all(Option::is_some)
    }

    /// Return `true` if all the blocks are absent, otherwise `false`.
    pub fn is_all_absent(&self) -> bool {
        self.stripe.iter().all(Option::is_none)
    }

    /// Set a block, and return the old value.
    ///
    /// # Parameters
    /// - `block_idx`: index of the block in a stripe
    /// - `block`: the block data to move the target block
    ///
    /// # Return
    /// the old value of the block
    ///
    /// # Panics
    /// - if `block_idx` is out of bounds
    pub fn replace_block(&mut self, block_idx: usize, block: Option<Block>) -> Option<Block> {
        let m = self.m();
        std::mem::replace(
            self.stripe
                .get_mut(block_idx)
                .unwrap_or_else(|| panic!("block index({block_idx}) is greater than m({})", m)),
            block,
        )
    }

    /// Make a [`PartialStripe`] with `k` source blocks and `p` parity blocks.
    /// All the blocks are sized with `block_size` and absent.
    pub fn make_absent_from_k_p(
        k: NonZeroUsize,
        p: NonZeroUsize,
        block_size: NonZeroUsize,
    ) -> Self {
        let k = k.get();
        let p = p.get();
        Self {
            k: k.try_into().unwrap(),
            p: p.try_into().unwrap(),
            stripe: vec![None; k + p],
            block_size: block_size.get(),
        }
    }

    /// Split the partial stripe by present and absent block
    ///
    /// # Returns
    /// A tuple with the present half and the absent half.
    /// Each half is a vector of tuples, composed of block index and reference to the block data.
    pub fn split_present_absent(&self) -> (PresentHalf, AbsentHalf) {
        let mut absent = Vec::with_capacity(self.absent_block_index().len());
        let mut present = Vec::with_capacity(self.m() - absent.len());
        for (idx, block_opt) in self.stripe.iter().enumerate() {
            match block_opt {
                Some(_) => present.push((idx, block_opt)),
                None => absent.push((idx, block_opt)),
            };
        }
        (present, absent)
    }

    /// Split the partial stripe by mutable present / absent block
    ///
    /// # Returns
    /// A tuple with the mutable present half and the mutable absent half.
    /// Each half is a vector of tuples, composed of block index and mutable reference to the block data.
    pub fn split_mut_present_absent(&mut self) -> (PresentHalfMut, AbsentHalfMut) {
        let mut absent = Vec::with_capacity(self.absent_block_index().len());
        let mut present = Vec::with_capacity(self.m() - absent.len());
        for (idx, block_opt) in self.stripe.iter_mut().enumerate() {
            match block_opt {
                Some(_) => present.push((idx, block_opt)),
                None => absent.push((idx, block_opt)),
            };
        }
        (present, absent)
    }

    /// Get the indexes of all the present blocks.
    pub fn present_block_index(&self) -> Vec<usize> {
        self.stripe
            .iter()
            .enumerate()
            .filter_map(|(idx, block_opt)| block_opt.is_some().then_some(idx))
            .collect()
    }

    /// Get the indexes of all the absent blocks.
    pub fn absent_block_index(&self) -> Vec<usize> {
        self.stripe
            .iter()
            .enumerate()
            .filter_map(|(idx, block_opt)| block_opt.is_none().then_some(idx))
            .collect()
    }
}

impl From<&Stripe> for PartialStripe {
    /// Make a [`PartialStripe`] **cloned** from a stripe.
    /// All the blocks are present.
    ///
    /// # Note
    /// This function implies data clone.
    fn from(stripe: &Stripe) -> Self {
        let block_size = stripe.block_size();
        let k = stripe.k();
        let p = stripe.p();
        let stripe = stripe.clone();
        Self {
            block_size,
            stripe: stripe.stripe.into_iter().map(Some).collect(),
            k: k.try_into().unwrap(),
            p: p.try_into().unwrap(),
        }
    }
}

impl From<Stripe> for PartialStripe {
    /// Make a [`PartialStripe`] moved from a stripe.
    fn from(stripe: Stripe) -> Self {
        let block_size = stripe.block_size();
        let k = stripe.k();
        let p = stripe.p();
        Self {
            block_size,
            stripe: stripe.stripe.into_iter().map(Some).collect(),
            k: k.try_into().unwrap(),
            p: p.try_into().unwrap(),
        }
    }
}
