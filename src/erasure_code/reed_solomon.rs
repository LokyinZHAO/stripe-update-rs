use std::num::NonZeroUsize;

use crate::{erasure_code::Block, SUError, SUResult};

use super::{check_partial_stripe_k_p, check_stripe_k_p, ErasureCode};

/// Make a reed-solomon erasure code instance.
pub struct ReedSolomon {
    /// number of source data
    k: usize,
    /// number of parity data
    p: usize,
    /// encode matrix, M * K
    encode_mat: Vec<u8>,
    /// encode table for parity
    encode_parity_table: Vec<u8>,
}

impl ReedSolomon {
    /// Make a [`ReedSolomon`]`(k+p, k)` erasure code.
    pub fn from_k_p(k: NonZeroUsize, p: NonZeroUsize) -> Self {
        let k = k.get();
        let p = p.get();
        let m = k + p;
        let encode_mat = isa_l::gf_gen_rs_matrix(k, m);
        let encode_parity_table = isa_l::ec_init_tables_owned(k, p, &encode_mat[(k * k)..]);
        Self {
            k,
            p,
            encode_mat,
            encode_parity_table,
        }
    }

    fn parity_delta_update(
        &self,
        source_slice: &[u8],
        source_idx: usize,
        parity_slice: &mut [&mut [u8]],
    ) -> SUResult<()> {
        parity_slice
            .iter_mut()
            .enumerate()
            .for_each(|(parity_idx, parity_slice)| {
                parity_slice
                    .iter_mut()
                    .zip(source_slice)
                    .for_each(|(p, &d)| {
                        let coef = self.encode_parity_table
                            [source_idx * 32 + parity_idx * self.k * 32 + 1];
                        *p ^= isa_l::gf_mul(d, coef);
                    });
            });
        Ok(())
    }
}

impl ErasureCode for ReedSolomon {
    /// number of the source block
    #[inline]
    fn k(&self) -> usize {
        self.k
    }
    /// number of the parity block
    #[inline]
    fn p(&self) -> usize {
        self.p
    }
    /// number of the source and parity block
    #[inline]
    fn m(&self) -> usize {
        self.k() + self.p()
    }
    /// Encode the full stripe, the source blocks will remain unmodified,
    /// and the parity blocks will be encoded from the source blocks.
    fn encode_stripe(&self, stripe: &mut super::Stripe) -> crate::SUResult<()> {
        check_stripe_k_p(self, stripe, file!(), line!(), column!())?;
        let len = stripe.block_size();
        let (source, parity) = stripe.split_mut_source_parity();
        isa_l::ec_encode_data(
            len,
            self.k(),
            self.p(),
            &self.encode_parity_table,
            source,
            parity,
        );
        Ok(())
    }
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
    /// - If `k` and `p` between this [`ReedSolomon`] erasure code and `partial_stripe` do not match
    fn decode(&self, partial_stripe: &mut super::PartialStripe) -> crate::SUResult<()> {
        check_partial_stripe_k_p(self, partial_stripe, file!(), line!(), column!())?;
        let block_size = partial_stripe.block_size();
        let (present, absent) = partial_stripe.split_mut_present_absent();
        if absent.len() > self.p {
            return Err(crate::SUError::erasure_code(
                (file!(), line!(), column!()),
                format!(
                    "cannot decode {} blocks from {} blocks by ({}, {}) rs code",
                    absent.len(),
                    present.len(),
                    self.m(),
                    self.k()
                ),
            ));
        }
        // select the first k survivors
        let (survivor_idx, survivor_block): (Vec<_>, Vec<_>) = present
            .iter()
            .take(self.k)
            .map(|(idx, block_opt)| (*idx, block_opt.as_ref().unwrap()))
            .unzip();
        let b = self
            .encode_mat
            .chunks_exact(self.k)
            .enumerate()
            .filter_map(|(i, chunk)| survivor_idx.contains(&i).then_some(chunk))
            .flatten()
            .copied()
            .collect::<Vec<u8>>();
        let inv_mat = isa_l::gf_invert_matrix(b).ok_or_else(|| {
            SUError::erasure_code(
                (file!(), line!(), column!()),
                format!(
                    "decode matrix in RS({}, {}) is invertible",
                    self.m(),
                    self.k(),
                ),
            )
        })?;
        // Get decode matrix with only wanted recovery rows
        let mut decode_mat: Vec<u8> = vec![0_u8; self.k * absent.len()];
        let k = self.k;
        decode_mat.chunks_exact_mut(k).zip(absent.iter()).for_each(
            |(decode_vec, (corrupt_idx, _))| {
                if *corrupt_idx < k {
                    // corrupted source block
                    decode_vec.copy_from_slice(&inv_mat[k * corrupt_idx..k * corrupt_idx + k]);
                } else {
                    // For non-src (parity) erasures need to multiply encode matrix * invert
                    decode_vec.iter_mut().enumerate().for_each(|(i, b)| {
                        *b = 0;
                        for j in 0..k {
                            *b ^= isa_l::gf_mul(
                                inv_mat[j * k + i],
                                self.encode_mat[k * corrupt_idx + j],
                            );
                        }
                    })
                }
            },
        );
        let decode_table = isa_l::ec_init_tables_owned(k, absent.len(), decode_mat);
        let mut to_recover = Block::zero_n(absent.len(), block_size);
        isa_l::ec_encode_data(
            block_size,
            k,
            absent.len(),
            &decode_table,
            survivor_block,
            &mut to_recover,
        );
        absent
            .into_iter()
            .zip(to_recover)
            .for_each(|((_, block), recover)| {
                let _ = std::mem::replace(block, Some(recover));
            });
        Ok(())
    }
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
    /// No certain state is guaranteed after any error occurs.
    /// - [SUError::ErasureCode] if not all the parity blocks are present
    /// - [SUError::ErasureCode] if the target source block to update is absent
    /// - [SUError::Range] if the `update_source_idx` is out of source block bound
    /// - [SUError::Range] if the updated area `[offset, offset + update_slice.len())` is out of block bound
    /// - [SUError::ErasureCode] if `k` and `p` between this [`ReedSolomon`] erasure code and `partial_stripe` do not match
    fn delta_update(
        &self,
        update_slice: &[u8],
        update_source_idx: usize,
        offset: usize,
        partial_stripe: &mut super::PartialStripe,
    ) -> crate::SUResult<()> {
        // check k p
        check_partial_stripe_k_p(self, partial_stripe, file!(), line!(), column!())?;
        // check range
        let valid_range = 0..partial_stripe.block_size();
        let range = offset..(offset + update_slice.len());
        if !valid_range.contains(&range.start) || !valid_range.contains(&(range.end - 1)) {
            return Err(SUError::out_of_range(
                (file!(), line!(), column!()),
                Some(valid_range),
                range,
            ));
        }
        let (source, parity) = partial_stripe.split_mut_source_parity();
        if !parity.iter().all(Option::is_some) {
            return Err(SUError::erasure_code(
                (file!(), line!(), column!()),
                "not all the parity blocks are present",
            ));
        }
        let target_source = source.get_mut(update_source_idx);
        if target_source.is_none() {
            return Err(SUError::out_of_range(
                (file!(), line!(), column!()),
                Some(valid_range),
                0..update_source_idx,
            ));
        }
        let target_source = target_source.unwrap();
        if target_source.is_none() {
            return Err(SUError::erasure_code(
                (file!(), line!(), column!()),
                format!("the target source block at {update_source_idx} is absent"),
            ));
        }
        let target_slice = &mut (target_source.as_mut().unwrap())[range.clone()];
        let delta = target_slice
            .iter()
            .zip(update_slice.iter())
            .map(|(a, b)| *a ^ *b)
            .collect::<Vec<_>>();
        let mut parity_slice = parity
            .iter_mut()
            .map(|block| &mut (block.as_mut().unwrap())[range.clone()])
            .collect::<Vec<_>>();
        self.parity_delta_update(&delta, update_source_idx, &mut parity_slice)?;
        target_slice.copy_from_slice(update_slice);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use super::super::test::*;
    use super::ReedSolomon;

    #[test]
    fn encode_decode() {
        let ec =
            ReedSolomon::from_k_p(NonZeroUsize::new(K).unwrap(), NonZeroUsize::new(P).unwrap());
        test_encode_decode(&ec);
    }

    #[test]
    fn delta_update() {
        let ec =
            ReedSolomon::from_k_p(NonZeroUsize::new(K).unwrap(), NonZeroUsize::new(P).unwrap());
        test_update(&ec);
    }
}
