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
        let source_location = format!("{}:{}:{}", file, line, column);
        return Err(SUError::range_not_match(
            source_location,
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
            format!("{file}:{line}:{column}"),
            valid_range,
            range,
        ));
    }
    Ok(())
}

/// Convert block id to its corresponding block file path
pub fn block_id_to_path(mut dev_root: PathBuf, block_id: BlockId) -> PathBuf {
    dev_root.push(block_id.to_string());
    dev_root
}

/// Convert block id to its corresponding block file path
pub fn block_path_to_id(block_path: &Path) -> BlockId {
    let file_name = block_path
        .file_name()
        .expect("invalid block path")
        .to_string_lossy();
    file_name.parse().expect("invalid block path")
}
