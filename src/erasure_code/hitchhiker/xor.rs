use std::{cmp::min, num::NonZeroUsize};

use itertools::Itertools;

use crate::{
    erasure_code::{self, Block, ErasureCode, ReedSolomon},
    SUError, SUResult,
};

use super::HitchhikerCode;

pub struct HitchhikerXor {
    rs: ReedSolomon,
}

impl HitchhikerXor {
    pub fn try_from_k_p(k: NonZeroUsize, p: NonZeroUsize) -> SUResult<Self> {
        if p.get() < 2 {
            return Err(SUError::erasure_code(
                (file!(), line!(), column!()),
                "p should be greater or equal than 2 with hitchhiker codes",
            ));
        }
        let rs = ReedSolomon::from_k_p(k, p);
        Ok(Self { rs })
    }
}

impl HitchhikerCode for HitchhikerXor {
    /// number of the source block
    #[inline]
    fn k(&self) -> usize {
        self.rs.k()
    }
    /// number of the parity block
    #[inline]
    fn p(&self) -> usize {
        self.rs.p()
    }
    /// number of the source and parity block
    #[inline]
    fn m(&self) -> usize {
        self.rs.m()
    }

    /// Encode the full stripe, the source blocks will remain unmodified,
    /// and the parity blocks will be encoded from the source blocks.
    ///
    /// # Note
    /// The `stripe` should have two sub-stripes continuously placed.
    ///
    /// For example, a (4+2) hitchhiker should receive a stripe like
    /// `a1, a2, a3, a4, a'5, a'6, b1, b2, b3, b4, b'5, b'6`.
    /// Where `a1~a5` and `b1~b5` are source blocks, while `a'5, a'6, b'5, b'6` are parity blocks.
    ///
    /// # Error
    /// - [`Err(SUError::ErasureCode)`] if `stripe.m` is not double of `self.m` or any other error occurs.
    fn encode_stripe(&self, stripe: &mut [erasure_code::Stripe]) -> SUResult<()> {
        if stripe.len() != 2 {
            return Err(SUError::erasure_code(
                (file!(), line!(), column!()),
                "hitchhiker-xor have 2 sub-stripes",
            ));
        }
        // split two sub-stripes
        stripe
            .iter_mut()
            .try_for_each(|sub_stripes| self.rs.encode_stripe(sub_stripes))?;
        let (a, b) = stripe
            .split_first_mut()
            .expect("a hitchhiker stripe should have at least two sub-stripes");
        let b = b
            .first_mut()
            .expect("a hitchhiker stripe should have at least two sub-stripes");
        // do xor
        use itertools::Itertools;
        let xor_group_num = self.p() - 1;
        let block_num_in_xor_group =
            self.k() / xor_group_num + if self.k() % xor_group_num == 0 { 0 } else { 1 };
        fn parity_xor(source: &Block, parity: &mut Block) {
            parity
                .iter_mut()
                .zip_eq(source.iter())
                .for_each(|(p, s)| *p ^= *s);
        }
        let a_chunked = a.as_source().chunks(block_num_in_xor_group);
        b.iter_mut_parity()
            .skip(1)
            .zip_eq(a_chunked)
            .for_each(|(parity, group_member)| {
                group_member
                    .iter()
                    .for_each(|source| parity_xor(source, parity))
            });
        Ok(())
    }
    /// Decode one absent blocks from the present blocks in the `partial_stripe`.
    /// If success, all the blocks in the `partial_stripe` will be present,
    /// otherwise the `partial_stripe` will remain unmodified.
    ///
    /// # Return
    /// - [`Ok`] if decode successfully, and all the blocks in the `partial_stripe` will be present.
    /// - [`Err(SUError::ErasureCode)`] if any error occurs, and the `partial_stripe` will remain unmodified.
    ///
    /// # Error
    /// - If the number of absent blocks are greater than the number of parity blocks.
    fn decode(&self, partial_stripe: &mut [erasure_code::PartialStripe]) -> SUResult<()> {
        // // split two sub-stripes
        // let (a, b) = partial_stripe
        //     .split_first_mut()
        //     .expect("a hitchhiker stripe should have at least two sub-stripes");
        // let b = b
        //     .first_mut()
        //     .expect("a hitchhiker stripe should have at least two sub-stripes");
        unimplemented!();
    }
    /// Repair an absent block from the present blocks in the `partial_stripe`s.
    ///
    /// # Error
    /// - If there are more than one absent blocks in the partial stripes.
    /// - If the indexes of the absent blocks in each sub-stripe are not consistent.
    fn repair(&self, partial_stripe: &mut [erasure_code::PartialStripe]) -> SUResult<()> {
        // split two sub-stripes
        let (a, b) = partial_stripe
            .split_first_mut()
            .expect("a hitchhiker stripe should have at least two sub-stripes");
        let b = b
            .first_mut()
            .expect("a hitchhiker stripe should have at least two sub-stripes");
        let a_absent_index = a.absent_block_index();
        let b_absent_index = b.absent_block_index();
        if a_absent_index.len() != 1 || b_absent_index.len() != 1 {
            return Err(SUError::erasure_code(
                (file!(), line!(), column!()),
                "there should be only one absent block in any sub-stripe for hitchhiker repairing",
            ));
        }
        let a_absent_index = a_absent_index[0];
        let b_absent_index = b_absent_index[0];
        if a_absent_index != b_absent_index {
            return Err(SUError::erasure_code(
                (file!(), line!(), column!()),
                "the indexes of the absent blocks in each sub-stripe are not consistent",
            ));
        }
        if b_absent_index >= self.k() {
            unimplemented!("repair parity not supported");
        }
        // repair b
        self.rs.decode(b)?;
        // repair a
        let (b_xor_parity, a_sources) = index_the_b_xor_parity(self, a_absent_index);
        let mut b_xor_parity = b.get(b_xor_parity).unwrap().clone();
        a_sources.iter().map(|i| a.get(*i).unwrap()).for_each(|a| {
            b_xor_parity
                .iter_mut()
                .zip_eq(a.iter())
                .for_each(|(b, a)| *b ^= *a)
        });
        let _ = a.replace_block(a_absent_index, Some(b_xor_parity));
        Ok(())
    }
}

/// get the corresponding xor parity in b sub-stripe, and the a sources
fn index_the_b_xor_parity(ec: &HitchhikerXor, absent_index: usize) -> (usize, Vec<usize>) {
    let k = ec.k();
    let p = ec.p();
    let xor_group_num = p - 1;
    let block_num_in_xor_group = k / xor_group_num + if k % xor_group_num == 0 { 0 } else { 1 };
    let parity_index = absent_index / block_num_in_xor_group;
    let a_source_begin = parity_index * block_num_in_xor_group;
    let a_sources = (a_source_begin..min(k - 1, a_source_begin + block_num_in_xor_group))
        .filter(|n| *n != absent_index)
        .collect::<Vec<_>>();
    (parity_index + p + 1, a_sources)
}
