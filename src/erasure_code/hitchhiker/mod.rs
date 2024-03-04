use crate::SUResult;

use super::{PartialStripe, Stripe};

mod xor;

pub use xor::HitchhikerXor;

pub trait HitchhikerCode {
    /// number of the source block
    fn k(&self) -> usize;
    /// number of the parity block
    fn p(&self) -> usize;
    /// number of the source and parity block
    fn m(&self) -> usize;
    /// Encode all the sub-stripes, the source blocks will remain unmodified,
    /// and the parity blocks will be encoded from the source blocks.
    /// # Note
    /// The `stripe` should have at least two sub-stripes continuously placed.
    ///
    /// For example, a (4+2) hitchhiker-xor should receive a stripe consisting two sub-stripes like
    /// `a1, a2, a3, a4, a'5, a'6, b1, b2, b3, b4, b'5, b'6`.
    /// Where `a1~a5` and `b1~b5` are source blocks, while `a'5, a'6, b'5, b'6` are parity blocks.
    ///
    /// # Error
    /// - [`Err(SUError::ErasureCode)`] if `stripe.m` is not multiple of `self.m` or any other error occurs.
    fn encode_stripe(&self, sub_stripe: &mut [Stripe]) -> SUResult<()>;
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
    fn decode(&self, partial_stripe: &mut [PartialStripe]) -> SUResult<()>;
    /// Repair an absent block from the present blocks in the `partial_stripe`s.
    ///
    /// # Error
    /// - If there are more than one absent blocks in the partial stripes.
    /// - If the indexes of the absent blocks in each sub-stripe are not consistent.
    fn repair(&self, partial_stripe: &mut [PartialStripe]) -> SUResult<()>;
}
