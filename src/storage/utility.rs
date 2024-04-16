use std::path::{Path, PathBuf};

use crate::{SUError, SUResult};

use super::BlockId;

/// Check if the data length matches the block size.
///
/// # Return
/// - [`Ok(())`] if `data_len` equals to block size
/// - [`Err(SUError::Range)`] if `data_len` does not equal to block size
pub fn check_block_range(
    file: &str,
    line: u32,
    column: u32,
    data_len: usize,
    block_size: usize,
) -> SUResult<()> {
    if data_len != block_size {
        return Err(SUError::range_not_match(
            (file, line, column),
            0..block_size,
            0..data_len,
        ));
    }
    Ok(())
}

/// Check if the range is in bound of the block.
///
/// # Parameter
/// - `source_location`: source_location of the method caller
/// - `range`: range to check
///
/// # Return
/// - [`Ok(())`] if `range` is in bound of the block
/// - [`Err(SUError::Range)`] if `range` is out of the bound
pub fn check_slice_range(
    file: &str,
    line: u32,
    column: u32,
    range: std::ops::Range<usize>,
    block_size: usize,
) -> SUResult<()> {
    let valid_range = 0..block_size;
    if !valid_range.contains(&range.start) || !valid_range.contains(&(range.end - 1)) {
        return Err(SUError::out_of_range(
            (file, line, column),
            Some(valid_range),
            range,
        ));
    }
    Ok(())
}

/// Convert block id to its corresponding block file path
pub fn block_id_to_path(dev_root: impl Into<PathBuf>, block_id: BlockId) -> PathBuf {
    let s = format!("{:04X}", block_id);
    let (a, b) = s.split_at(2);
    let mut dev_root = dev_root.into();
    dev_root.push(a);
    dev_root.push(b);
    dev_root
}

/// Convert block id to its corresponding block file path
///
/// # Panics
/// If the path is not constructed by [`block_id_to_path`]
#[allow(dead_code)]
pub fn block_path_to_id(block_path: &Path) -> BlockId {
    const ERR_STR: &str = "invalid block path";
    let mut path = block_path.to_path_buf();
    let b = path
        .file_name()
        .expect(ERR_STR)
        .to_string_lossy()
        .to_string();
    if !path.pop() {
        panic!("{ERR_STR}");
    }
    let a = path
        .file_name()
        .expect(ERR_STR)
        .to_string_lossy()
        .to_string();
    let s = a + b.as_str();
    usize::from_str_radix(&s, 16).expect(ERR_STR)
}

#[cfg(test)]
mod test {
    use rand::Rng;

    use crate::storage::utility::{block_id_to_path, block_path_to_id};
    use std::str::FromStr;

    #[test]
    fn test_block_id_to_path() {
        let block_id: usize = 256;
        let root = std::path::PathBuf::from_str("./root").unwrap();
        let path = block_id_to_path(root.clone(), block_id);
        assert_eq!(path, std::path::PathBuf::from_str("./root/01/00").unwrap());
        let block_id_reconstruct = block_path_to_id(&path);
        assert_eq!(block_id_reconstruct, block_id);
        (0..10000)
            .map(|_| rand::thread_rng().gen::<usize>())
            .for_each(|id| {
                let path = block_id_to_path(root.clone(), id);
                let r_id = block_path_to_id(&path);
                assert_eq!(r_id, id);
            })
    }
}
