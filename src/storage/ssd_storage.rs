use std::{
    fs::File,
    io::{Read, Seek, Write},
    num::NonZeroUsize,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use crate::{SUError, SUResult};

use super::{
    check_block_range,
    lru_evict::LruEvict,
    utility::{block_id_to_path, block_path_to_id, check_slice_range},
    BlockId, BlockStorage, EvictStrategy, HDDStorage, SliceStorage,
};

pub struct SSDStorage {
    dev: PathBuf,
    block_size: usize,
    next_storage: HDDStorage,
    evict: LruEvict<PathBuf>,
}

impl SSDStorage {
    /// Connect the [`SSDStorage`] to a device(supposed to be a SSD device) to store the block.
    /// The number of blocks stored in ssd is bounded,
    /// and some blocks will be evicted to an unbounded storage if the number of block blocks exceeds.
    ///
    /// # Parameter
    /// - `dev_path`: path to the HDD device
    /// - `block_size`: size of each block to be created
    /// - `max_block_num`: maximum number of block stored in ssd
    /// - `next_storage`: the unbounded storage to store the exceeding blocks
    ///
    /// # Error
    /// [`SUError::Io(std::io::ErrorKind::NotFound)`] if `dev_path` not existing
    pub fn connect_to_dev(
        dev_path: PathBuf,
        block_size: NonZeroUsize,
        max_block_num: NonZeroUsize,
        next_storage: HDDStorage,
    ) -> SUResult<Self> {
        if !dev_path.exists() {
            return Err(SUError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dev path not found",
            )));
        }
        Ok(Self {
            dev: dev_path,
            block_size: block_size.get(),
            next_storage,
            evict: LruEvict::with_capacity(max_block_num),
        })
    }

    /// Open an existing block file.
    /// If the block does not exist in ssd, it will then try to fetch the block in the next storage layer,
    ///
    /// # Return
    /// - [`Ok(Some)`] on success with the [`File`] returned
    /// - [`Ok(None)`] on the block not existing in both ssd storage and the next storage layer
    /// - [`Err`] on any error occurring
    ///
    /// # Note
    /// This method may evict any existing block file to maintain the ssd storage size.
    fn open_block(&self, block_id: BlockId) -> SUResult<Option<File>> {
        let block_file_path = block_id_to_path(self.dev.to_owned(), block_id);
        if self.evict.contains(&block_file_path) {
            let f = File::options()
                .write(true)
                .read(true)
                .open(block_file_path.as_path())?;
            Ok(Some(f))
        } else {
            // the block does not exist, try to fetch from the next storage layer
            self.next_storage
                .get_block_owned(block_id)?
                .map(|block| self.make_block_from_data(block_file_path.as_path(), &block))
                .transpose()
        }
    }

    /// Try to open an existing block file.
    /// If the block does not exist in ssd, it will then try to fetch the block in the next storage layer.
    /// If the block does not exist in both ssd and the next storage layer, it will create a new block in ssd.
    /// Thus, this method will always return a block file if no error occurs.
    ///
    /// # Return
    /// - [`Ok`] on successfully opening or creating the block with the [`File`] returned
    /// - [`Err`] on any error occurring
    ///
    /// # Note
    /// This method may evict any existing block file to maintain the ssd storage size.
    fn open_or_create_block(&self, block_id: BlockId) -> SUResult<File> {
        match self.open_block(block_id) {
            Ok(Some(f)) => Ok(f),
            Ok(None) => {
                // try to make a new block
                let block_path = block_id_to_path(self.dev.to_owned(), block_id);
                self.make_block_zero(block_path.as_path())
            }
            Err(e) => Err(e),
        }
    }

    /// Make a block filled with the given data.
    /// This method will create a block file if it does not exist,
    /// and will fail if it already exists.
    ///
    /// # Return
    /// - [`Ok`] on successfully creating a new block file or overwriting an existing file with the block file returned
    /// - [`Err`] on any error occurs
    ///
    /// # Error
    /// - [`SUError::Range`] if `block_data.len()` does not match block size
    /// - [`SUError::Io`] if the block file already exists
    ///
    /// # Note
    /// This method may evict existing block file to maintain the ssd size.
    fn make_block_from_data(&self, block_path: &Path, block_data: &[u8]) -> SUResult<File> {
        check_block_range(
            file!(),
            line!(),
            column!(),
            block_data.len(),
            self.block_size,
        )?;
        // create a block file and fill it with data
        let mut f = self.make_block_zero(block_path)?;
        f.write_all(block_data)?;
        f.seek(std::io::SeekFrom::Start(0))?;
        Ok(f)
    }

    /// Try to make a new block filled with `0`.
    /// It fails if the block file already exists.
    ///
    /// # Return
    /// - [`Ok`] on successfully creating a new block file with the block file returned
    /// - [`Err`] on any error occurs
    ///
    /// # Error
    /// - [`SUError::Range`] if `block_data.len()` does not match block size
    /// - [`SUError::Io`] if the block file already exists
    ///
    /// # Note
    /// This method may evict existing block file to maintain the ssd size.
    fn make_block_zero(&self, block_path: &Path) -> SUResult<File> {
        // create a block file
        std::fs::create_dir_all(block_path.parent().unwrap())?;
        let f = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(block_path)
            .unwrap();
        f.set_len(self.block_size.try_into().unwrap())?;
        // evict block file if necessary
        if let Some(evict) = self.evict.push(block_path.to_owned()) {
            let mut evict_file = File::open(evict.as_path())?;
            let mut evict_data = vec![0_u8; self.block_size];
            evict_file.read_exact(&mut evict_data)?;
            self.next_storage
                .put_block(block_path_to_id(evict.as_path()), &evict_data)?;
            std::fs::remove_file(evict.as_path())?;
        }
        Ok(f)
    }
}

impl BlockStorage for SSDStorage {
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
    ///
    /// # Error
    /// - [SUError::Range] if `block_data.len()` does not match block size
    fn put_block(&self, block_id: super::BlockId, block_data: &[u8]) -> crate::SUResult<()> {
        check_block_range(
            file!(),
            line!(),
            column!(),
            block_data.len(),
            self.block_size,
        )?;
        let mut f = self.open_or_create_block(block_id)?;
        f.write_all(block_data)?;
        Ok(())
    }

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
    fn get_block(
        &self,
        block_id: super::BlockId,
        block_data: &mut [u8],
    ) -> crate::SUResult<Option<()>> {
        check_block_range(
            file!(),
            line!(),
            column!(),
            block_data.len(),
            self.block_size,
        )?;
        self.open_block(block_id)?
            .map(|mut f| f.read_exact(block_data))
            .transpose()
            .map_err(SUError::from)
    }

    /// Get size of a block
    fn block_size(&self) -> usize {
        self.block_size
    }
}

impl SliceStorage for SSDStorage {
    fn put_slice(
        &self,
        block_id: super::BlockId,
        inner_block_offset: usize,
        slice_data: &[u8],
    ) -> crate::SUResult<Option<()>> {
        let slice_range = inner_block_offset..inner_block_offset + slice_data.len();
        // check range
        check_slice_range(
            file!(),
            line!(),
            column!(),
            slice_range.clone(),
            self.block_size(),
        )?;
        self.open_block(block_id)?
            .map(|f| f.write_all_at(slice_data, slice_range.start.try_into().unwrap()))
            .transpose()
            .map_err(SUError::from)
    }

    fn get_slice(
        &self,
        block_id: super::BlockId,
        inner_block_offset: usize,
        slice_data: &mut [u8],
    ) -> crate::SUResult<Option<()>> {
        let slice_range = inner_block_offset..inner_block_offset + slice_data.len();
        // check range
        check_slice_range(
            file!(),
            line!(),
            column!(),
            slice_range.clone(),
            self.block_size(),
        )?;
        self.open_block(block_id)?
            .map(|f| f.read_exact_at(slice_data, slice_range.start.try_into().unwrap()))
            .transpose()
            .map_err(SUError::from)
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;
    use std::num::NonZeroUsize;

    use crate::{
        storage::{BlockStorage, SliceStorage},
        SUError,
    };

    use super::{HDDStorage, SSDStorage};
    const BLOCK_SIZE: usize = 4 << 10;
    const BLOCK_NUM: usize = 4 << 10;
    const SSD_CAP_NUM: usize = BLOCK_NUM / 3;
    fn random_block_data() -> Vec<u8> {
        rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(BLOCK_SIZE)
            .collect()
    }

    #[test]
    fn put_get_block() {
        let hdd_dev = tempfile::TempDir::new().unwrap();
        let ssd_dev = tempfile::TempDir::new().unwrap();
        let hdd_store = HDDStorage::connect_to_dev(
            hdd_dev.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let ssd_store = SSDStorage::connect_to_dev(
            ssd_dev.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
            NonZeroUsize::new(SSD_CAP_NUM).unwrap(),
            hdd_store,
        )
        .unwrap();
        let blocks = (0..BLOCK_NUM)
            .map(|_| random_block_data())
            .collect::<Vec<_>>();
        // put blocks
        blocks
            .iter()
            .enumerate()
            .for_each(|(i, block)| ssd_store.put_block(i, block).unwrap());
        // get blocks
        blocks.iter().enumerate().for_each(|(i, block)| {
            let data = ssd_store.get_block_owned(i).unwrap().unwrap();
            assert_eq!(&data, block);
        });
        let mut data = vec![0_u8; BLOCK_SIZE];
        blocks.iter().enumerate().for_each(|(i, block)| {
            ssd_store.get_block(i, &mut data).unwrap().unwrap();
            assert_eq!(&data, block);
        });
        // update
        let update_blocks = (0..BLOCK_NUM)
            .step_by(3)
            .map(|i| (i, random_block_data()))
            .collect::<Vec<_>>();
        update_blocks
            .iter()
            .for_each(|(i, block)| ssd_store.put_block(*i, block).unwrap());
        update_blocks.iter().for_each(|(i, block)| {
            let retrieve = ssd_store.get_block_owned(*i).unwrap().unwrap();
            assert_eq!(block, &retrieve);
        })
    }

    #[test]
    fn block_error_handle() {
        let hdd_dev = tempfile::TempDir::new().unwrap();
        let ssd_dev = tempfile::TempDir::new().unwrap();
        let hdd_store = HDDStorage::connect_to_dev(
            hdd_dev.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let ssd_store_err = HDDStorage::connect_to_dev(
            std::path::PathBuf::from("404"),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap_err();
        assert_eq!(
            ssd_store_err.into_io_err().unwrap().kind(),
            std::io::ErrorKind::NotFound
        );
        let store = SSDStorage::connect_to_dev(
            ssd_dev.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
            NonZeroUsize::new(SSD_CAP_NUM).unwrap(),
            hdd_store,
        )
        .unwrap();
        // put blocks out of range
        let out_of_range_data = vec![0_u8; BLOCK_SIZE + 1];
        let e = store.put_block(0, &out_of_range_data).unwrap_err();
        assert!(matches!(e, SUError::Range(_)));
        let out_of_range_data = vec![0_u8; BLOCK_SIZE - 1];
        let e = store.put_block(0, &out_of_range_data).unwrap_err();
        assert!(matches!(e, SUError::Range(_)));

        // get block out of range
        let mut out_of_range_data = vec![0_u8; BLOCK_SIZE + 1];
        let e = store.get_block(0, &mut out_of_range_data).unwrap_err();
        assert!(matches!(e, SUError::Range(_)));
        let mut out_of_range_data = vec![0_u8; BLOCK_SIZE - 1];
        let e = store.get_block(0, &mut out_of_range_data).unwrap_err();
        assert!(matches!(e, SUError::Range(_)));
        // get block not exists
        let mut data = vec![0_u8; BLOCK_SIZE];
        let ret = store.get_block(0, &mut data).unwrap();
        assert!(ret.is_none());

        // get block owned not exists
        let ret = store.get_block_owned(9).unwrap();
        assert!(ret.is_none());
    }

    #[test]
    fn put_get_slice() {
        let hdd_dev = tempfile::TempDir::new().unwrap();
        let ssd_dev = tempfile::TempDir::new().unwrap();
        let hdd_store = HDDStorage::connect_to_dev(
            hdd_dev.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let ssd_store = SSDStorage::connect_to_dev(
            ssd_dev.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
            NonZeroUsize::new(SSD_CAP_NUM).unwrap(),
            hdd_store,
        )
        .unwrap();
        let blocks = (0..BLOCK_NUM)
            .map(|_| random_block_data())
            .collect::<Vec<_>>();
        // put blocks
        blocks
            .iter()
            .enumerate()
            .for_each(|(i, block)| ssd_store.put_block(i, block).unwrap());
        // get slice
        fn random_slice_range() -> std::ops::Range<usize> {
            let start = rand::thread_rng().gen_range(0..BLOCK_SIZE - 1);
            let end = rand::thread_rng().gen_range(start..BLOCK_SIZE);
            start..end
        }
        fn random_slice_data(range: std::ops::Range<usize>) -> Vec<u8> {
            rand::thread_rng()
                .sample_iter(rand::distributions::Standard)
                .take(range.len())
                .collect()
        }
        let get_slice_range = (0..blocks.len())
            .map(|_| random_slice_range())
            .collect::<Vec<_>>();
        get_slice_range
            .iter()
            .enumerate()
            .map(|(i, range)| {
                let owned_data = ssd_store
                    .get_slice_owned(i, range.to_owned())
                    .unwrap()
                    .unwrap();
                let mut data = vec![0_u8; range.len()];
                ssd_store
                    .get_slice(i, range.start, &mut data)
                    .unwrap()
                    .unwrap();
                assert_eq!(&owned_data, &data);
                (range, owned_data)
            })
            .zip(blocks.iter())
            .for_each(|((range, retrieve), expect)| {
                assert_eq!(&expect[range.to_owned()], &retrieve)
            });
        // update slice
        let update_slice = (0..BLOCK_NUM)
            .step_by(2)
            .map(|i| {
                let range = random_slice_range();
                let slice_data = random_slice_data(range.clone());
                (i, range, slice_data)
            })
            .collect::<Vec<_>>();
        update_slice.iter().for_each(|(i, range, slice_data)| {
            ssd_store
                .put_slice(*i, range.start, slice_data)
                .unwrap()
                .unwrap()
        });
        update_slice
            .iter()
            .map(|(i, range, slice_data)| {
                let expect = slice_data;
                let retrieved_owned = ssd_store
                    .get_slice_owned(*i, range.clone())
                    .unwrap()
                    .unwrap();
                let mut retrieved = vec![0_u8; range.len()];
                ssd_store
                    .get_slice(*i, range.start, &mut retrieved)
                    .unwrap()
                    .unwrap();
                assert_eq!(expect, &retrieved);
                (expect, retrieved_owned)
            })
            .for_each(|(expect, retrieved)| assert_eq!(expect, &retrieved));
        let mut updated_block = blocks.clone();
        update_slice.iter().for_each(|(i, range, slice_data)| {
            updated_block.get_mut(*i).unwrap()[range.clone()].copy_from_slice(&slice_data)
        });
        updated_block
            .iter()
            .enumerate()
            .map(|(i, expect)| (expect, ssd_store.get_block_owned(i).unwrap().unwrap()))
            .for_each(|(expect, retrieved)| assert_eq!(expect, &retrieved));
    }

    #[test]
    fn slice_error_handle() {
        let ssd_store = tempfile::tempdir().unwrap();
        let hdd_dev = tempfile::tempdir().unwrap();
        let hdd_store = HDDStorage::connect_to_dev(
            hdd_dev.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let ssd_store = SSDStorage::connect_to_dev(
            ssd_store.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
            NonZeroUsize::new(SSD_CAP_NUM).unwrap(),
            hdd_store,
        )
        .unwrap();
        let blocks = (0..BLOCK_NUM)
            .map(|_| random_block_data())
            .collect::<Vec<_>>();
        // put blocks
        blocks
            .iter()
            .enumerate()
            .for_each(|(i, block)| ssd_store.put_block(i, block).unwrap());
        // get 404
        let e = ssd_store.get_slice_owned(BLOCK_NUM, 0..1).unwrap();
        assert!(e.is_none());
        // get invalid range
        let e = ssd_store.get_slice_owned(0, 0..BLOCK_SIZE + 1);
        assert!(matches!(e, Err(SUError::Range(_))));
        let e = ssd_store.get_slice_owned(0, BLOCK_SIZE..BLOCK_SIZE + 1);
        assert!(matches!(e, Err(SUError::Range(_))));
        // put 404
        let data = vec![0_u8; BLOCK_SIZE * 2];
        let e = ssd_store
            .put_slice(BLOCK_NUM, 0, &data[0..BLOCK_SIZE])
            .unwrap();
        assert!(e.is_none());
        // put offset out of range
        let e = ssd_store.put_slice(BLOCK_NUM - 1, BLOCK_SIZE, &data[0..1]);
        assert!(matches!(e, Err(SUError::Range(_))));
        // put slice len out of range
        let e = ssd_store.put_slice(BLOCK_NUM - 1, BLOCK_SIZE - 1, &data[0..2]);
        assert!(matches!(e, Err(SUError::Range(_))));
        let e = ssd_store.put_slice(BLOCK_NUM - 1, 0, &data[0..BLOCK_SIZE + 1]);
        assert!(matches!(e, Err(SUError::Range(_))));
    }
}
