use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    io::{Read, Seek, Write},
    num::NonZeroUsize,
    path::PathBuf,
};

use crate::{
    storage::{utility::block_id_to_path, PartialBlock, SliceOpt},
    SUError, SUResult,
};

use super::{evict::RangeSet, BlockId, BufferEviction, EvictStrategySlice, MostModifiedBlockEvict};

type SegId = usize;
type RecordIdx = usize;
const SEG_SIZE: usize = 4 << 10;

#[derive(Debug)]
pub struct FixedSizeSliceBuf<E = MostModifiedBlockEvict>
where
    E: std::fmt::Debug,
{
    evict: E,
    dev_dir: PathBuf,
    block_size: usize,
    seg_map: RefCell<HashMap<BlockId, std::collections::BTreeMap<SegId, RecordIdx>>>,
}

impl<E> FixedSizeSliceBuf<E>
where
    E: std::fmt::Debug,
{
    pub fn cleanup_dev(&self) -> SUResult<()> {
        for entry in self.dev_dir.read_dir()? {
            let dir = entry?.path();
            std::fs::remove_dir(dir.as_path())?;
        }
        Ok(())
    }
}

impl<E> FixedSizeSliceBuf<E>
where
    E: EvictStrategySlice,
{
    pub fn connect_to_dev_with_evict(
        dev_root: impl Into<PathBuf>,
        block_size: NonZeroUsize,
        evict: E,
    ) -> SUResult<Self> {
        let dev_root = dev_root.into();
        if !dev_root.exists() {
            return Err(SUError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dev path not found",
            )));
        }
        Ok(Self {
            evict,
            dev_dir: dev_root,
            block_size: block_size.get(),
            seg_map: Default::default(),
        })
    }
}

impl FixedSizeSliceBuf<MostModifiedBlockEvict> {
    pub fn connect_to_dev(
        dev_root: impl Into<PathBuf>,
        block_size: NonZeroUsize,
        capacity: NonZeroUsize,
    ) -> SUResult<Self> {
        let dev_root = dev_root.into();
        if !dev_root.exists() {
            return Err(SUError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dev path not found",
            )));
        }
        Ok(Self {
            evict: MostModifiedBlockEvict::with_max_size(capacity),
            dev_dir: dev_root,
            block_size: block_size.get(),
            seg_map: Default::default(),
        })
    }
}

impl<E> FixedSizeSliceBuf<E>
where
    E: std::fmt::Debug,
{
    /// Make an eviction from the block id.
    /// The record file and the log for this block will also be removed.
    ///
    /// # Panics
    /// - Any underlying os error occurs.
    fn make_buffer_eviction(&self, block_id: BlockId, ranges: RangeSet) -> BufferEviction {
        let seg_map = self.seg_map.borrow_mut().remove(&block_id).unwrap();
        let path = super::block_id_to_path(self.dev_dir.to_owned(), block_id);
        let mut f = std::fs::File::open(path.as_path()).unwrap();
        let mut buf = bytes::BytesMut::zeroed(ranges.len());
        let mut slices: Vec<SliceOpt> =
            vec![SliceOpt::Absent(SEG_SIZE); self.block_size / SEG_SIZE];
        let mut segs = seg_map
            .into_iter()
            .map(|(id, record_index)| (record_index, id))
            .collect::<Vec<_>>();
        segs.sort_unstable_by_key(|(record_index, _)| *record_index);
        assert!(segs.iter().enumerate().all(|(i, (idx, _))| i == *idx));
        segs.iter().for_each(|(_record_index, seg_id)| {
            let mut slice_buf = buf.split_to(SEG_SIZE);
            f.read_exact(&mut slice_buf).unwrap();
            slices[*seg_id] = SliceOpt::Present(slice_buf.freeze());
        });
        std::fs::remove_file(path).unwrap();
        BufferEviction {
            block_id,
            data: PartialBlock {
                size: self.block_size,
                slices,
            },
        }
    }
}

impl<E> Drop for FixedSizeSliceBuf<E>
where
    E: std::fmt::Debug,
{
    fn drop(&mut self) {
        self.cleanup_dev().unwrap_or_else(|e| {
            eprintln!(
                "fail to clean up dev root:{}, error: {e}",
                self.dev_dir.display()
            )
        });
    }
}

impl<E> super::SliceBuffer for FixedSizeSliceBuf<E>
where
    E: EvictStrategySlice,
{
    fn push_slice(
        &self,
        block_id: BlockId,
        inner_block_offset: usize,
        slice_data: &[u8],
    ) -> SUResult<Option<super::BufferEviction>> {
        // assert the slice is aligned with segment size
        let slice_range = inner_block_offset..inner_block_offset + slice_data.len();
        let seg_range = slice_range.start / SEG_SIZE..slice_range.end / SEG_SIZE;
        assert_eq!(slice_range.start % SEG_SIZE, 0);
        assert_eq!(slice_range.end % SEG_SIZE, 0);
        let eviction = self.evict.push(block_id, slice_range.clone());
        // put data
        let mut update_buf_map = self.seg_map.borrow_mut();
        if cfg!(debug_assertions) {
            // check map and storage is consistent
            let map_path = update_buf_map
                .iter()
                .map(|(id, _)| block_id_to_path(self.dev_dir.as_path(), *id))
                .collect::<std::collections::BTreeSet<_>>();
            let storage = walkdir::WalkDir::new(self.dev_dir.as_path())
                .into_iter()
                .map(|p| p.unwrap().path().to_path_buf())
                .filter(|p| p.is_file())
                .collect::<std::collections::BTreeSet<_>>();
            let diff = map_path
                .difference(&storage)
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>();
            assert!(
                diff.is_empty(),
                "map > storage, diff: {}",
                diff.first().unwrap()
            );
            let diff = storage
                .difference(&map_path)
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>();
            assert!(
                diff.is_empty(),
                "map < storage, diff: {}",
                diff.first().unwrap()
            );
        }
        let path = super::block_id_to_path(self.dev_dir.to_owned(), block_id);
        if let Some(map_record) = update_buf_map.get_mut(&block_id) {
            let mut f = std::fs::File::options()
                .read(true)
                .write(true)
                .open(path.as_path())
                .unwrap();
            slice_data
                .chunks_exact(SEG_SIZE)
                .zip(seg_range)
                .try_for_each(|(data, seg_id)| {
                    if let Some(idx) = map_record.get(&seg_id) {
                        // existing segment, update
                        f.seek(std::io::SeekFrom::Start(
                            u64::try_from(*idx * SEG_SIZE).unwrap(),
                        ))
                        .unwrap();
                        f.write_all(data)?;
                    } else {
                        // new segment, append
                        f.seek(std::io::SeekFrom::End(0)).unwrap();
                        f.write_all(data)?;
                        let val = map_record.insert(seg_id, map_record.len());
                        debug_assert!(val.is_none());
                    }
                    Ok::<(), SUError>(())
                })?;
        } else {
            // put a new block record
            let mut btree_map = BTreeMap::new();
            (seg_range).enumerate().for_each(|(i, seg_id)| {
                let val = btree_map.insert(seg_id, i);
                debug_assert!(val.is_none());
            });
            let val = update_buf_map.insert(block_id, btree_map);
            debug_assert!(val.is_none());
            std::fs::create_dir_all(path.parent().unwrap())?;
            debug_assert!(!path.exists());
            let mut f = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(path)
                .unwrap();
            f.write_all(slice_data)?;
        }
        drop(update_buf_map);
        Ok(eviction.map(|evict| self.make_buffer_eviction(evict.0, evict.1)))
    }

    fn pop(&self) -> Option<super::BufferEviction> {
        self.evict
            .pop_first()
            .map(|evict| self.make_buffer_eviction(evict.0, evict.1))
    }

    fn len(&self) -> usize {
        self.evict.len()
    }

    fn pop_one(&self, block_id: BlockId) -> Option<BufferEviction> {
        self.evict
            .pop_with_id(block_id)
            .map(|evict| self.make_buffer_eviction(block_id, evict))
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, num::NonZeroUsize};

    use rand::Rng;

    use crate::storage::{
        utility::block_id_to_path, BlockId, BufferEviction, EvictStrategySlice, PartialBlock,
        SliceBuffer,
    };

    use super::{FixedSizeSliceBuf, SEG_SIZE};

    const BLOCK_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(SEG_SIZE * 20) };
    const CAPACITY: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(BLOCK_SIZE.get() * 4) };
    const BLOCK_NUM: usize = CAPACITY.get() / BLOCK_SIZE.get() * 2;
    const SLICE_SIZE: usize = SEG_SIZE;
    const TEST_LOAD: usize = CAPACITY.get() * 4 / SLICE_SIZE;
    #[test]
    fn test_fixed_size_buf() {
        let tempfile = tempfile::tempdir().unwrap();
        let dev_root = tempfile.path();
        let slice_buf = FixedSizeSliceBuf::connect_to_dev(dev_root, BLOCK_SIZE, CAPACITY).unwrap();
        let blocks = vec![vec![None::<u8>; BLOCK_SIZE.get()]; BLOCK_NUM];
        let blocks = RefCell::new(blocks);
        let check_evict = |evict: Option<BufferEviction>| {
            if let Some(BufferEviction { block_id, data }) = evict {
                let PartialBlock { size, slices } = data;
                assert!(!block_id_to_path(dev_root, block_id).exists());
                assert_eq!(size, BLOCK_SIZE.get());
                let block_ref = std::mem::replace(
                    &mut blocks.borrow_mut()[block_id],
                    vec![None; BLOCK_SIZE.get()],
                );
                let mut offset = 0;
                slices.iter().for_each(|slice| match slice {
                    crate::storage::SliceOpt::Present(slice_get) => {
                        let slice_ref = block_ref[offset..offset + slice_get.len()]
                            .iter()
                            .map(|b| b.as_ref().unwrap().to_owned())
                            .collect::<Vec<_>>();
                        assert_eq!(slice_ref[..], slice_get[..]);
                        offset += slice_get.len();
                    }
                    crate::storage::SliceOpt::Absent(size) => {
                        assert!(block_ref[offset..offset + size].iter().all(Option::is_none));
                        offset += size;
                    }
                });
            }
        };
        (0..TEST_LOAD)
            .map(|_| {
                let block_id: BlockId = rand::thread_rng().gen_range(0..BLOCK_NUM);
                let start = rand::thread_rng().gen_range(0..BLOCK_SIZE.get());
                let end = rand::thread_rng().gen_range(start..BLOCK_SIZE.get());
                let offset = start / SLICE_SIZE * SLICE_SIZE;
                let len = std::cmp::max(SLICE_SIZE, (end - start) / SLICE_SIZE * SLICE_SIZE);
                let slice_data = rand::thread_rng()
                    .sample_iter(rand::distributions::Standard)
                    .take(len)
                    .collect::<Vec<u8>>();
                (block_id, offset, slice_data)
            })
            .inspect(|(block_id, offset, slice_data)| {
                (&mut blocks.borrow_mut()[*block_id][*offset..*offset + slice_data.len()])
                    .iter_mut()
                    .zip(slice_data)
                    .for_each(|(a, b)| *a = Some(*b));
            })
            .for_each(|(block_id, offset, slice_data)| {
                let evict = slice_buf.push_slice(block_id, offset, &slice_data).unwrap();
                check_evict(evict);
            });
        let buf_len = slice_buf.evict.len();
        assert!(
            buf_len <= CAPACITY.get(),
            "buf_len: {}, CAPACITY :{}",
            buf_len,
            CAPACITY.get()
        );
        assert!(
            buf_len >= (CAPACITY.get() * 8 / 10),
            "buf_len: {}, .9*CAPACITY: {}",
            buf_len,
            CAPACITY.get() * 9 / 10
        );
        while let Some(evict) = slice_buf.pop() {
            check_evict(Some(evict))
        }
        assert!(slice_buf.evict.is_empty());
    }

    #[test]
    fn fixed_size_buf_error_handle() {}
}
