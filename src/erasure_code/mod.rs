mod block;
mod reed_solomon;
mod stripe;

pub use block::Block;
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
    /// That is, only the modified area of the source blocks are computed to delta,
    /// and the corresponding area of the parity blocks are updated by the delta.
    ///
    /// Typically this method can be much more faster than full stripe update via [`Self::encode_stripe()`].
    fn delta_update(&self, partial_stripe: &mut PartialStripe) -> SUResult<()>;
}

/// check the k and p matches between erasure code interface and the stripe
fn check_k_p(
    ec: &dyn ErasureCode,
    stripe: &Stripe,
    file: &str,
    line: u32,
    column: u32,
) -> SUResult<()> {
    let ec = (ec.k(), ec.p());
    let stripe = (stripe.k(), stripe.p());
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
        assert_eq!(stripes, recovered);
    }

    pub fn test_update(ec: &dyn ErasureCode) {
        let mut stripes = gen_stripes();
        stripes
            .iter_mut()
            .for_each(|stripe| ec.encode_stripe(stripe).unwrap());
    }
}
