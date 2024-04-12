use std::fs::File;
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use crate::SUError;
use crate::SUResult;

use super::utility::check_slice_range;
use super::utility::{block_id_to_path, check_block_range};
use super::{BlockId, BlockStorage, SliceStorage};

#[derive(Debug)]
pub struct HDDStorage {
    dev: std::path::PathBuf,
    block_size: usize,
}

impl HDDStorage {
    /// Connect the [`HDDStorage`] to a device(supposed to be a HDD device) to store the block.
    ///
    /// # Parameter
    /// - `dev_path`: path to the HDD device
    /// - `block_size`: size of each block to be created
    ///
    /// # Error
    /// [`SUError::Io(std::io::ErrorKind::NotFound)`] if `dev_path` not existing
    pub fn connect_to_dev(
        dev_path: impl Into<PathBuf>,
        block_size: NonZeroUsize,
    ) -> SUResult<Self> {
        let dev_path: PathBuf = dev_path.into();
        if !dev_path.exists() {
            return Err(SUError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dev path not found",
            )));
        }
        let block_size = block_size.get();
        Ok(Self {
            dev: dev_path,
            block_size,
        })
    }

    /// Open a block file.
    ///
    /// # Return
    /// - [`Ok(Some)`] on success with the [`File`] returned
    /// - [`Ok(None)`] on the block not existing
    /// - [`Err`] on any error occurring
    fn open_block(&self, block_id: BlockId) -> SUResult<Option<File>> {
        match File::options()
            .write(true)
            .read(true)
            .open(block_id_to_path(self.dev.to_owned(), block_id))
        {
            Ok(f) => Ok(Some(f)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SUError::Io(e)),
        }
    }

    /// Get the path of the device root
    pub fn get_dev_root(&self) -> &std::path::Path {
        &self.dev
    }

    /// Create a new block file, guaranteed to be new and with block size
    ///
    /// # Return
    /// - [`Ok`] on success with the [`File`] returned.
    /// - [`Err`] on any error occurring
    ///
    /// # Error
    /// - It is an error if the block file already exists
    fn create_block(&self, block_id: BlockId) -> SUResult<File> {
        let file_path = block_id_to_path(self.dev.to_owned(), block_id);
        let parent_dir = file_path.parent().unwrap();
        std::fs::create_dir_all(parent_dir)?;
        match File::options()
            .write(true)
            .read(true)
            .create_new(true)
            .open(file_path)
        {
            Ok(f) => {
                f.set_len(self.block_size.try_into().unwrap())?;
                Ok(f)
            }
            Err(e) => Err(SUError::Io(e)),
        }
    }
}

impl BlockStorage for HDDStorage {
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
        let f = match self.open_block(block_id)? {
            Some(f) => f,
            None => {
                // block does not exits, creating a new block
                self.create_block(block_id)?
            }
        };
        f.write_all_at(block_data, 0)?;
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
            .map(|f| f.read_exact_at(block_data, 0))
            .transpose()
            .map_err(SUError::Io)
    }

    /// Get size of a block
    fn block_size(&self) -> usize {
        self.block_size
    }
}

impl SliceStorage for HDDStorage {
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
    ) -> SUResult<Option<()>> {
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
    ) -> SUResult<Option<()>> {
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

    use super::HDDStorage;
    const BLOCK_SIZE: usize = 4 << 10;
    const BLOCK_NUM: usize = 4 << 10;
    fn random_block_data() -> Vec<u8> {
        rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(BLOCK_SIZE)
            .collect()
    }

    #[test]
    fn put_get_block() {
        let tempfile = tempfile::TempDir::new().unwrap();
        let hdd_store = HDDStorage::connect_to_dev(
            tempfile.path().to_owned(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let blocks = (0..BLOCK_NUM)
            .map(|_| random_block_data())
            .collect::<Vec<_>>();
        // put blocks
        blocks
            .iter()
            .enumerate()
            .for_each(|(i, block)| hdd_store.put_block(i, block).unwrap());
        // get blocks
        blocks.iter().enumerate().for_each(|(i, block)| {
            let data = hdd_store.get_block_owned(i).unwrap().unwrap();
            assert_eq!(&data, block);
        });
        let mut data = vec![0_u8; BLOCK_SIZE];
        blocks.iter().enumerate().for_each(|(i, block)| {
            hdd_store.get_block(i, &mut data).unwrap().unwrap();
            assert_eq!(&data, block);
        });
        // update
        let update_blocks = (0..BLOCK_NUM)
            .step_by(3)
            .map(|i| (i, random_block_data()))
            .collect::<Vec<_>>();
        update_blocks
            .iter()
            .for_each(|(i, block)| hdd_store.put_block(*i, block).unwrap());
        update_blocks.iter().for_each(|(i, block)| {
            let retrieve = hdd_store.get_block_owned(*i).unwrap().unwrap();
            assert_eq!(block, &retrieve);
        })
    }

    #[test]
    fn block_error_handle() {
        let hdd_store_err = HDDStorage::connect_to_dev(
            std::path::PathBuf::from("404"),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap_err();
        assert_eq!(
            hdd_store_err.into_io_err().unwrap().kind(),
            std::io::ErrorKind::NotFound
        );

        let tempfile = tempfile::TempDir::new().unwrap();
        let hdd_store = HDDStorage::connect_to_dev(
            tempfile.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        // put blocks out of range
        let out_of_range_data = vec![0_u8; BLOCK_SIZE + 1];
        let e = hdd_store.put_block(0, &out_of_range_data).unwrap_err();
        assert!(matches!(e, SUError::Range(_)));
        let out_of_range_data = vec![0_u8; BLOCK_SIZE - 1];
        let e = hdd_store.put_block(0, &out_of_range_data).unwrap_err();
        assert!(matches!(e, SUError::Range(_)));

        // get block out of range
        let mut out_of_range_data = vec![0_u8; BLOCK_SIZE + 1];
        let e = hdd_store.get_block(0, &mut out_of_range_data).unwrap_err();
        assert!(matches!(e, SUError::Range(_)));
        let mut out_of_range_data = vec![0_u8; BLOCK_SIZE - 1];
        let e = hdd_store.get_block(0, &mut out_of_range_data).unwrap_err();
        assert!(matches!(e, SUError::Range(_)));
        // get block not exists
        let mut data = vec![0_u8; BLOCK_SIZE];
        let ret = hdd_store.get_block(0, &mut data).unwrap();
        assert!(ret.is_none());

        // get block owned not exists
        let ret = hdd_store.get_block_owned(9).unwrap();
        assert!(ret.is_none());
    }

    #[test]
    fn put_get_slice() {
        let tempdir = tempfile::tempdir().unwrap();
        let hdd_store = HDDStorage::connect_to_dev(
            tempdir.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let blocks = (0..BLOCK_NUM)
            .map(|_| random_block_data())
            .collect::<Vec<_>>();
        // put blocks
        blocks
            .iter()
            .enumerate()
            .for_each(|(i, block)| hdd_store.put_block(i, block).unwrap());
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
                let owned_data = hdd_store
                    .get_slice_owned(i, range.to_owned())
                    .unwrap()
                    .unwrap();
                let mut data = vec![0_u8; range.len()];
                hdd_store
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
            hdd_store
                .put_slice(*i, range.start, slice_data)
                .unwrap()
                .unwrap()
        });
        update_slice
            .iter()
            .map(|(i, range, slice_data)| {
                let expect = slice_data;
                let retrieved_owned = hdd_store
                    .get_slice_owned(*i, range.clone())
                    .unwrap()
                    .unwrap();
                let mut retrieved = vec![0_u8; range.len()];
                hdd_store
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
            .map(|(i, expect)| (expect, hdd_store.get_block_owned(i).unwrap().unwrap()))
            .for_each(|(expect, retrieved)| assert_eq!(expect, &retrieved));
    }

    #[test]
    fn slice_error_handle() {
        let tempdir = tempfile::tempdir().unwrap();
        let hdd_store = HDDStorage::connect_to_dev(
            tempdir.path().to_path_buf(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let blocks = (0..BLOCK_NUM)
            .map(|_| random_block_data())
            .collect::<Vec<_>>();
        // put blocks
        blocks
            .iter()
            .enumerate()
            .for_each(|(i, block)| hdd_store.put_block(i, block).unwrap());
        // get 404
        let e = hdd_store.get_slice_owned(BLOCK_NUM, 0..1).unwrap();
        assert!(e.is_none());
        // get invalid range
        let e = hdd_store.get_slice_owned(0, 0..BLOCK_SIZE + 1);
        assert!(matches!(e, Err(SUError::Range(_))));
        let e = hdd_store.get_slice_owned(0, BLOCK_SIZE..BLOCK_SIZE + 1);
        assert!(matches!(e, Err(SUError::Range(_))));
        // put 404
        let data = vec![0_u8; BLOCK_SIZE * 2];
        let e = hdd_store
            .put_slice(BLOCK_NUM, 0, &data[0..BLOCK_SIZE])
            .unwrap();
        assert!(e.is_none());
        // put offset out of range
        let e = hdd_store.put_slice(BLOCK_NUM - 1, BLOCK_SIZE, &data[0..1]);
        assert!(matches!(e, Err(SUError::Range(_))));
        // put slice len out of range
        let e = hdd_store.put_slice(BLOCK_NUM - 1, BLOCK_SIZE - 1, &data[0..2]);
        assert!(matches!(e, Err(SUError::Range(_))));
        let e = hdd_store.put_slice(BLOCK_NUM - 1, 0, &data[0..BLOCK_SIZE + 1]);
        assert!(matches!(e, Err(SUError::Range(_))));
    }
}
