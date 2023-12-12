#[allow(unused_imports)]
use crate::SUError;
use crate::SUResult;

mod hdd_storage;
mod lru_evict;
mod ssd_storage;
mod utility;

pub use hdd_storage::HDDStorage;
pub use ssd_storage::SSDStorage;

pub type BlockId = usize;

use utility::*;

pub trait BlockStorage {
    /// Storing data to a block.
    /// A new block will be created if the block does not exist.
    ///
    /// # Parameter
    /// - `block_id`: id of the block
    /// - `block_data`: data of the block to store
    ///
    /// # Return
    /// - [`Ok`]: on success
    /// - [`Err`]: on any error occurring
    fn put_block(&self, block_id: BlockId, block_data: &[u8]) -> SUResult<()>;
    /// Retrieving data from a full block.
    ///
    /// # Parameter
    /// - `block_id`: id of the block
    /// - `block_data`: buffer to get the block data
    ///
    /// # Return
    /// - [`Ok(Some)`] on success, and the buffer `block_data` filled with the corresponding data
    /// - [`Ok(None)`] on block not existing
    /// - [`Err`] on any error occurring
    ///
    /// # Error
    /// - [`SUError::Range`] if `block_data.len()` does not match the block length
    fn get_block(&self, block_id: BlockId, block_data: &mut [u8]) -> SUResult<Option<()>>;
    /// Retrieving data from a full block.
    ///
    /// # Parameter
    /// - `block_id`: id of the block
    ///
    /// # Return
    /// - [`Ok(Some)`] on success with the corresponding block data returned
    /// - [`Ok(None)`] on block not existing
    /// - [`Err`] on any error occurring
    fn get_block_owned(&self, block_id: BlockId) -> SUResult<Option<Vec<u8>>> {
        let mut data = vec![0_u8; self.block_size()];
        self.get_block(block_id, &mut data)
            .map(|opt| opt.map(|_| data))
    }
    /// Get size of a block
    fn block_size(&self) -> usize;
}

pub trait SliceStorage: BlockStorage {
    /// Storing data from a slice to a specific area of a block.
    /// The block area to store is defined as `Block[inner_block_offset, inner_block_offset + slice_data.len())`.
    ///
    /// # Parameter
    /// - `block_id`: id of the block
    /// - `inner_block_offset`: offset from the start of the block
    /// - `slice_data`: data of the slice to store
    ///
    /// # Return
    /// - [`Ok(Some)`] on success
    /// - [`Ok(None)`] on block not existing
    /// - [`Err`] on any error occurring
    ///
    /// # Error
    /// - [SUError::Range] if the area specified is out of the block range
    fn put_slice(
        &self,
        block_id: BlockId,
        inner_block_offset: usize,
        slice_data: &[u8],
    ) -> SUResult<Option<()>>;
    /// Retrieving slice data from a specific area of a block to a slice buffer.
    /// The block area to retrieve is defined as `Block[inner_block_offset, inner_block_offset + slice_data.len()`).
    ///
    /// # Return
    /// - [`Ok(Some)`] on success, and the buffer `slice_data` with be filled with the corresponding data.
    /// - [`Ok(None)`] on block not existing
    /// - [`Err`] on any error occurring
    ///
    /// # Error
    /// - [SUError::Range] if the area specified is out of the block range
    fn get_slice(
        &self,
        block_id: BlockId,
        inner_block_offset: usize,
        slice_data: &mut [u8],
    ) -> SUResult<Option<()>>;
    /// Retrieving slice data from a specific area of a block.
    /// The block area to retrieve is defined as `Block[range.start..range.end)`
    ///
    /// # Return
    /// - [`Ok(Some)`] on success with the corresponding slice data returned
    /// - [`Ok(None)`] on block not existing
    /// - [`Err`] on any error occurring
    ///
    /// # Error
    /// - [SUError::Range] if the area specified is out of the block range
    fn get_slice_owned(
        &self,
        block_id: BlockId,
        range: std::ops::Range<usize>,
    ) -> SUResult<Option<Vec<u8>>> {
        let mut data: Vec<u8> = vec![0_u8; range.len()];
        self.get_slice(block_id, range.start, data.as_mut_slice())
            .map(|opt| opt.map(|_| data))
    }
}

trait EvictStrategy {
    type Item;
    /// Return `true` if the evict contains an element equal to `item`, otherwise false
    fn contains(&self, item: &Self::Item) -> bool;
    /// Push an item into the container.
    /// If the container is full, it returns the evicted item, other wise `None`
    fn push(&self, item: Self::Item) -> Option<Self::Item>;
    /// Pop an item from the container.
    /// If the container is empty, it returns `None`.
    fn pop(&self) -> Option<Self::Item>;
}
