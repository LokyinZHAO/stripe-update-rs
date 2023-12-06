use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::{fs::File, path::PathBuf};

use crate::SUError;
use crate::SUResult;

use super::{BlockId, BlockStorage};

#[derive(Debug)]
pub struct HDDStorage {
    dir: std::path::PathBuf,
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
    pub fn connect_to_dev(dev_path: &std::path::Path, block_size: NonZeroUsize) -> SUResult<Self> {
        if !dev_path.exists() {
            return Err(SUError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dev path not found",
            )));
        }
        let block_size = block_size.get();
        Ok(Self {
            dir: dev_path.to_owned(),
            block_size,
        })
    }

    fn block_id_to_path(&self, block_id: BlockId) -> PathBuf {
        let mut dir = self.dir.clone();
        dir.push(block_id.to_string());
        dir
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
            .open(self.block_id_to_path(block_id))
        {
            Ok(f) => Ok(Some(f)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SUError::Io(e)),
        }
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
        match File::options()
            .write(true)
            .read(true)
            .create_new(true)
            .open(self.block_id_to_path(block_id))
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
    fn put_block(
        &self,
        block_id: super::BlockId,
        block_data: impl AsRef<[u8]>,
    ) -> crate::SUResult<()> {
        let block_data = block_data.as_ref();
        if block_data.len() != self.block_size {
            return Err(SUError::range_not_match(
                "hdd storage put block",
                0..self.block_size,
                0..block_data.len(),
            ));
        }
        let f = match self.open_block(block_id)? {
            Some(f) => f,
            None => {
                // block does not exits
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
        mut block_data: impl AsMut<[u8]>,
    ) -> crate::SUResult<Option<()>> {
        let mut block_data = block_data.as_mut();
        if block_data.len() != self.block_size {
            if block_data.len() != self.block_size {
                return Err(SUError::range_not_match(
                    "hdd storage get block",
                    0..self.block_size,
                    0..block_data.len(),
                ));
            }
        }
        self.open_block(block_id)?
            .map(|f| f.read_exact_at(&mut block_data, 0))
            .transpose()
            .map_err(SUError::Io)
    }

    /// Retrieving data from a full block.
    ///
    /// # Parameter
    /// - `block_id`: id of the block
    ///
    /// # Return
    /// - [`Ok(Some)`] on success with the corresponding block data returned
    /// - [`Ok(None)`] on block not existing
    /// - [`Err`] on any error occurring
    fn get_block_owned(&self, block_id: super::BlockId) -> crate::SUResult<Option<Vec<u8>>> {
        let mut block_data = Vec::with_capacity(self.block_size);
        unsafe { block_data.set_len(self.block_size) };
        Ok(self
            .get_block(block_id, &mut block_data)?
            .map(|_| block_data))
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use crate::{storage::BlockStorage, SUError};

    use super::HDDStorage;
    const BLOCK_SIZE: usize = 4 << 10;
    const BLOCK_NUM: usize = 4 << 10;
    fn random_block_data() -> Vec<u8> {
        use rand::Rng;
        rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(BLOCK_SIZE)
            .collect()
    }

    #[test]
    fn put_get() {
        let tempfile = tempfile::TempDir::new().unwrap();
        let hdd_store =
            HDDStorage::connect_to_dev(tempfile.path(), NonZeroUsize::new(BLOCK_SIZE).unwrap())
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
    }

    #[test]
    fn error_handle() {
        let hdd_store_err = HDDStorage::connect_to_dev(
            std::path::PathBuf::from("404").as_path(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
        )
        .unwrap_err();
        assert_eq!(
            hdd_store_err.into_io_err().unwrap().kind(),
            std::io::ErrorKind::NotFound
        );

        let tempfile = tempfile::TempDir::new().unwrap();
        let hdd_store =
            HDDStorage::connect_to_dev(tempfile.path(), NonZeroUsize::new(BLOCK_SIZE).unwrap())
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
}
