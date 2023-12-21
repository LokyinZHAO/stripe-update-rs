use crate::SUResult;

mod evict;
mod hdd_storage;
mod slice_buffer;
mod ssd_storage;
mod utility;

pub use evict::EvictStrategySlice;
pub use evict::MostModifiedBlockEvict;
pub use evict::MostModifiedStripeEvict;
pub use hdd_storage::HDDStorage;
pub use slice_buffer::FixedSizeSliceBuf;
pub use ssd_storage::SSDStorage;

pub type BlockId = usize;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct StripeId(usize); // use new type pattern to avoid confusion with BlockId
impl From<usize> for StripeId {
    fn from(value: usize) -> Self {
        StripeId(value)
    }
}
impl StripeId {
    pub fn into_inner(self) -> usize {
        self.0
    }
}
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

pub trait SliceStorage {
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

pub struct BufferEviction {
    pub block_id: BlockId,
    pub data: PartialBlock,
}

pub trait SliceBuffer {
    /// Push a slice to the buffer.
    /// The slice is treated as part of a block at range`[inner_block_offset..inner_block_offset + slice_data.len())`
    /// If part of the slice is already in the buffer, it will be updated.
    /// And the non-existing part of the slice will be inserted.
    ///
    /// # Note
    /// The size of the buffer is typically fixed, therefor, any slice put may cause an eviction.
    ///
    /// # Return
    /// - [`Ok(Some)`] if the slice is successfully put into the buffer, and an eviction occurs
    /// - [`Ok(None)`] if the slice is successfully put into the buffer, and no eviction occurs
    /// - [`Err`] if any error occurs
    fn push_slice(
        &self,
        block_id: BlockId,
        inner_block_offset: usize,
        slice_data: &[u8],
    ) -> SUResult<Option<BufferEviction>>;

    fn pop(&self) -> Option<BufferEviction>;
    fn pop_one(&self, block_id: BlockId) -> Option<BufferEviction>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone)]
pub enum SliceOpt {
    /// data of the present slice
    Present(bytes::Bytes),
    /// size of the absent slice
    Absent(usize),
}

pub struct PartialBlock {
    pub size: usize,
    pub slices: Vec<SliceOpt>,
}
