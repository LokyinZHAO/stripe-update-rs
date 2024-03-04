mod block;
mod hitchhiker;
mod reed_solomon;
mod stripe;

pub use block::Block;
pub use hitchhiker::{HitchhikerCode, HitchhikerXor};
pub use reed_solomon::ReedSolomon;
pub use stripe::PartialStripe;
pub use stripe::Stripe;

use crate::{SUError, SUResult};

pub trait ErasureCode {
    /// number of the source block
    fn k(&self) -> usize;
    /// number of the parity block
    fn p(&self) -> usize;
    /// number of the source and parity block
    fn m(&self) -> usize;
    /// Encode the full stripe, the source blocks will remain unmodified,
    /// and the parity blocks will be encoded from the source blocks.
    fn encode_stripe(&self, stripe: &mut Stripe) -> SUResult<()>;
    /// Decode the absent blocks from the present blocks in the `partial_stripe`.
    /// If success, all the blocks in the `partial_stripe` will be present,
    /// otherwise the `partial_stripe` will remain unmodified.
    ///
    /// # Return
    /// - [`Ok`] if decode successfully, and all the blocks in the `partial_stripe` will be present.
    /// - [`Err(SUError::ErasureCode)`] if any error occurs, and the `partial_stripe` will remain unmodified.
    ///
    /// # Error
    /// - If the number of absent blocks are greater than the number of parity blocks.
    fn decode(&self, partial_stripe: &mut PartialStripe) -> SUResult<()>;
    /// Update the stripe in delta manner.
    /// That is, only the area `[offset, offset + update_slice.len())` of the source block
    /// at `update_source_idx` are updated to the content of `update_slice`.
    /// And then, the delta are computed and all the corresponding area of the parity blocks
    /// are updated by the delta.
    ///
    /// Typically this method can be much more faster than full stripe update via [`Self::encode_stripe()`].
    ///
    /// # Parameters
    /// - `update_slice`: the content to copy to the target source block
    /// - `update_source_idx`: the index of the source block to update in a stripe
    /// - `offset`: the start of the region to update
    /// - `partial_stripe`: partial stripe to update, all the parity blocks should be present,
    /// and will be updated source blocks.
    ///
    /// # Error
    /// - [SUError::ErasureCode] if not all the parity blocks are present
    /// - [SUError::Range] if the `update_source_idx` is out of source block bound
    /// - [SUError::Range] if the updated area `[offset, offset + update_slice.len())` is out of block bound
    fn delta_update(
        &self,
        update_slice: &[u8],
        update_source_idx: usize,
        offset: usize,
        partial_stripe: &mut PartialStripe,
    ) -> SUResult<()>;
}

/// check the k and p matches between erasure code interface and the `partial_stripe`
fn check_partial_stripe_k_p(
    ec: &dyn ErasureCode,
    partial_stripe: &PartialStripe,
    file: &str,
    line: u32,
    column: u32,
) -> SUResult<()> {
    check_k_p(
        ec,
        partial_stripe.k(),
        partial_stripe.p(),
        file,
        line,
        column,
    )
}
/// check the k and p matches between erasure code interface and the `stripe`
fn check_stripe_k_p(
    ec: &dyn ErasureCode,
    stripe: &Stripe,
    file: &str,
    line: u32,
    column: u32,
) -> SUResult<()> {
    check_k_p(ec, stripe.k(), stripe.p(), file, line, column)
}
fn check_k_p(
    ec: &dyn ErasureCode,
    k: usize,
    p: usize,
    file: &str,
    line: u32,
    column: u32,
) -> SUResult<()> {
    let ec = (ec.k(), ec.p());
    let stripe = (k, p);
    if ec.0 != stripe.0 {
        Err(SUError::erasure_code(
            (file, line, column),
            "k does not match between erasure code interface and stripe",
        ))
    } else if ec.1 != stripe.1 {
        Err(SUError::erasure_code(
            (file, line, column),
            "p does not match between erasure code interface and stripe",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use rand::Rng;

    use crate::erasure_code::Stripe;

    use super::{ErasureCode, PartialStripe};

    pub const K: usize = 4;
    pub const P: usize = 2;
    pub const M: usize = K + P;
    pub const BLOCK_SIZE: usize = 4 << 10;
    pub const STRIPE_NUM: usize = 1 << 10;

    pub fn gen_stripes() -> Vec<Stripe> {
        (0..STRIPE_NUM)
            .map(|_| {
                Stripe::zero(
                    NonZeroUsize::new(K).unwrap(),
                    NonZeroUsize::new(P).unwrap(),
                    NonZeroUsize::new(BLOCK_SIZE).unwrap(),
                )
            })
            .map(|mut stripe| {
                stripe.iter_mut_source().for_each(|block| {
                    block
                        .iter_mut()
                        .for_each(|byte| *byte = rand::thread_rng().gen())
                });
                stripe
            })
            .collect()
    }

    pub fn test_encode_decode(ec: &dyn ErasureCode) {
        let stripes = {
            let mut s = gen_stripes();
            s.iter_mut()
                .for_each(|stripe| ec.encode_stripe(stripe).unwrap());
            s
        };
        let corrupt_idx: Vec<Vec<_>> = (0..stripes.len())
            .map(|_| {
                // randomly corrupt 1~p blocks
                let corrupt_num = rand::thread_rng().gen_range(1..=P);
                let mut corrupt_idx = (0..corrupt_num)
                    .map(|_| rand::thread_rng().gen_range(0..M))
                    .collect::<Vec<_>>();
                corrupt_idx.sort();
                corrupt_idx.dedup();
                corrupt_idx
            })
            .collect();
        let corrupt_stripe = stripes
            .clone()
            .into_iter()
            .map(PartialStripe::from)
            .zip(corrupt_idx)
            .map(|(mut stripe, corrupt)| {
                // corrupt blocks
                corrupt.iter().for_each(|idx| {
                    stripe.replace_block(*idx, None);
                });
                stripe
            })
            .collect::<Vec<_>>();
        // recover the corrupted stripe
        let recovered = corrupt_stripe
            .into_iter()
            .map(|mut s| {
                ec.decode(&mut s).unwrap();
                Stripe::try_from(s).unwrap()
            })
            .collect::<Vec<_>>();
        stripes
            .iter()
            .zip(recovered.iter())
            .for_each(|(a, b)| assert_stripe_eq(a, b));
    }

    fn stripe_update(ec: &dyn ErasureCode, stripe: &Stripe) {
        let mut rng = rand::thread_rng();
        let range = {
            let start = rand::thread_rng().gen_range(0..BLOCK_SIZE / 2);
            let end = rand::thread_rng().gen_range(start + 1..BLOCK_SIZE);
            start..end
        };
        let update_slice = (0..K)
            .map(|_| {
                rng.gen_bool(0.4).then(|| {
                    rand::thread_rng()
                        .sample_iter(rand::distributions::Standard)
                        .take(range.len())
                        .collect::<Vec<u8>>()
                })
            })
            .collect::<Vec<_>>();
        let modified_stripe = {
            let mut stripe = stripe.clone();
            stripe
                .iter_mut_source()
                .zip(update_slice.iter())
                .for_each(|(block, update_slice)| {
                    if let Some(slice) = update_slice {
                        block[range.clone()].copy_from_slice(slice)
                    };
                });
            stripe
        };
        let expect = {
            let mut s = modified_stripe.clone();
            ec.encode_stripe(&mut s).unwrap();
            s
        };
        let result: Stripe = {
            let mut s = PartialStripe::from(stripe);
            let (source, _) = s.split_source_parity();
            assert_eq!(source.len(), update_slice.len());
            update_slice
                .iter()
                .enumerate()
                .filter(|(_, update)| update.is_some())
                .for_each(|(idx, update)| {
                    let update = update.as_ref().unwrap();
                    ec.delta_update(&update, idx, range.start, &mut s).unwrap();
                });
            Stripe::try_from(s).unwrap()
        };
        assert_stripe_eq(&expect, &result);
    }

    pub fn test_update(ec: &dyn ErasureCode) {
        let mut stripes = gen_stripes();
        stripes
            .iter_mut()
            .for_each(|stripe| ec.encode_stripe(stripe).unwrap());
        stripes.iter().for_each(|stripe| stripe_update(ec, stripe));
    }

    fn assert_stripe_eq(a: &Stripe, b: &Stripe) {
        assert_eq!(a.k(), b.k());
        assert_eq!(a.p(), b.p());
        for (i, (a, b)) in a
            .as_source()
            .iter()
            .chain(a.as_parity())
            .zip(b.as_source().iter().chain(b.as_parity()))
            .enumerate()
        {
            if a != b {
                let a_hex = hex::encode(a);
                let b_hex = hex::encode(b);
                panic!(
                    "the {i}-th block not match:\n
                    a:{}\n
                    b:{}\n",
                    a_hex, b_hex
                );
            }
        }
    }
}
