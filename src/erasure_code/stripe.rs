use std::num::NonZeroUsize;

use bytes::{BufMut, BytesMut};

use crate::SUError;

use super::Block;

#[derive(Debug, PartialEq, Eq)]
pub struct Stripe {
    k: u8,
    p: u8,
    stripe: Vec<Block>,
}

impl Stripe {
    #[inline]
    pub fn k(&self) -> usize {
        self.k.try_into().unwrap()
    }

    #[inline]
    pub fn p(&self) -> usize {
        self.p.try_into().unwrap()
    }

    #[inline]
    pub fn m(&self) -> usize {
        self.k() + self.p()
    }

    #[inline]
    pub fn block_size(&self) -> usize {
        self.stripe.first().unwrap().block_size()
    }

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

    pub fn split_source_parity(&self) -> (&[Block], &[Block]) {
        self.stripe.split_at(self.k())
    }

    pub fn split_mut_source_parity(&mut self) -> (&mut [Block], &mut [Block]) {
        let k = self.k();
        self.stripe.split_at_mut(k)
    }

    pub fn as_source(&self) -> &[Block] {
        let k = self.k();
        &self.stripe[0..k]
    }

    pub fn as_mut_source(&mut self) -> &mut [Block] {
        let k = self.k();
        &mut self.stripe[0..k]
    }

    pub fn as_parity(&self) -> &[Block] {
        let k = self.k();
        let m = self.m();
        &self.stripe[k..m]
    }

    pub fn as_mut_parity(&mut self) -> &mut [Block] {
        let k = self.k();
        let m = self.m();
        &mut self.stripe[k..m]
    }

    pub fn iter_source(&self) -> impl ExactSizeIterator<Item = &Block> {
        let k = self.k();
        self.stripe[0..k].iter()
    }

    pub fn iter_mut_source(&mut self) -> impl ExactSizeIterator<Item = &mut Block> {
        let k = self.k();
        self.stripe[0..k].iter_mut()
    }
}

impl Clone for Stripe {
    fn clone(&self) -> Self {
        let mut buf = BytesMut::with_capacity(self.m() * self.block_size());
        let block_size = self.block_size();
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

    fn try_from(value: PartialStripe) -> Result<Self, Self::Error> {
        if !value.is_all_present() {
            return Err(Self::Error::erasure_code(
                (file!(), line!(), column!()),
                "not all the blocks are present",
            ));
        }
        let k = value.k;
        let p = value.p;
        let stripe = value
            .stripe
            .into_iter()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        Ok(Self { k, p, stripe })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PartialStripe {
    block_size: usize,
    stripe: Vec<Option<Block>>,
    k: u8,
    p: u8,
}

type PartialStripeSplit<'a> = (
    Vec<(usize, &'a Option<Block>)>,
    Vec<(usize, &'a Option<Block>)>,
);

type PartialStripeSplitMut<'a> = (
    Vec<(usize, &'a mut Option<Block>)>,
    Vec<(usize, &'a mut Option<Block>)>,
);

impl PartialStripe {
    #[inline]
    pub fn k(&self) -> usize {
        self.k.try_into().unwrap()
    }

    #[inline]
    pub fn p(&self) -> usize {
        self.p.try_into().unwrap()
    }

    #[inline]
    pub fn m(&self) -> usize {
        self.k() + self.p()
    }

    #[inline]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn is_all_present(&self) -> bool {
        self.stripe.iter().all(Option::is_some)
    }

    pub fn is_all_absent(&self) -> bool {
        self.stripe.iter().all(Option::is_none)
    }

    pub fn set_block(&mut self, block_idx: usize, block: Option<Block>) -> Option<Block> {
        let m = self.m();
        std::mem::replace(
            self.stripe
                .get_mut(block_idx)
                .unwrap_or_else(|| panic!("block index({block_idx}) is greater than m({})", m)),
            block,
        )
    }

    pub fn absent_from_k_p(k: NonZeroUsize, p: NonZeroUsize, block_size: NonZeroUsize) -> Self {
        let k = k.get();
        let p = p.get();
        Self {
            k: k.try_into().unwrap(),
            p: p.try_into().unwrap(),
            stripe: vec![None; k + p],
            block_size: block_size.get(),
        }
    }

    /// Split the partial stripe by present / absent
    pub fn split(&self) -> PartialStripeSplit {
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

    /// Split the partial stripe by present / absent
    pub fn split_mut(&mut self) -> PartialStripeSplitMut {
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

    pub fn present_block_index(&self) -> Vec<usize> {
        self.stripe
            .iter()
            .enumerate()
            .filter_map(|(idx, block_opt)| block_opt.is_some().then_some(idx))
            .collect()
    }

    pub fn absent_block_index(&self) -> Vec<usize> {
        self.stripe
            .iter()
            .enumerate()
            .filter_map(|(idx, block_opt)| block_opt.is_none().then_some(idx))
            .collect()
    }

    pub fn iter_present(&self) -> impl Iterator<Item = (usize, &Block)> {
        self.stripe
            .iter()
            .enumerate()
            .filter_map(|(idx, block_opt)| block_opt.as_ref().map(|block| (idx, block)))
    }
}

impl From<&Stripe> for PartialStripe {
    fn from(value: &Stripe) -> Self {
        let block_size = value.block_size();
        let k = value.k();
        let p = value.p();
        let stripe = value.clone();
        Self {
            block_size,
            stripe: stripe.stripe.into_iter().map(Some).collect(),
            k: k.try_into().unwrap(),
            p: p.try_into().unwrap(),
        }
    }
}

impl From<Stripe> for PartialStripe {
    fn from(value: Stripe) -> Self {
        let block_size = value.block_size();
        let k = value.k();
        let p = value.p();
        Self {
            block_size,
            stripe: value.stripe.into_iter().map(Some).collect(),
            k: k.try_into().unwrap(),
            p: p.try_into().unwrap(),
        }
    }
}
