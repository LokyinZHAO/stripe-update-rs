use std::path::PathBuf;

use crate::{storage::BlockId, SUResult};

mod baseline;
// mod dist_merge;
mod dryrun;
mod merge_stripe;

#[derive(Debug, Default, serde::Deserialize, Clone, clap::ValueEnum)]
pub enum Manner {
    /// No optimization, ssd fetches and updates in block unit.
    #[default]
    Baseline,
    /// Merge the updates of a stripe
    MergeStripe,
    /// No disk write/read is performed, only generate and report disk access trace.
    TraceDryRun,
}

impl std::fmt::Display for Manner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Manner::Baseline => f.write_str("baseline"),
            Manner::MergeStripe => f.write_str("merge_stripe"),
            Manner::TraceDryRun => f.write_str("trace_dryrun"),
        }
    }
}

#[derive(Debug, Default)]
pub struct Bench {
    block_size: Option<usize>,
    block_num: Option<usize>,
    ssd_block_cap: Option<usize>,
    ssd_dev_path: Option<PathBuf>,
    blob_dev_path: Option<PathBuf>,
    k_p: Option<(usize, usize)>,
    test_num: Option<usize>,
    slice_size: Option<usize>,
    out_dir_path: Option<PathBuf>,
    manner: Manner,
}

impl Bench {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block_size(&mut self, block_size: usize) -> &mut Self {
        self.block_size = Some(block_size);
        self
    }

    pub fn block_num(&mut self, block_num: usize) -> &mut Self {
        self.block_num = Some(block_num);
        self
    }

    pub fn ssd_block_capacity(&mut self, ssd_block_capacity: usize) -> &mut Self {
        self.ssd_block_cap = Some(ssd_block_capacity);
        self
    }

    pub fn ssd_dev_path(&mut self, ssd_dev_path: impl Into<PathBuf>) -> &mut Self {
        self.ssd_dev_path = Some(ssd_dev_path.into());
        self
    }

    pub fn blob_dev_path(&mut self, blob_dev_path: impl Into<PathBuf>) -> &mut Self {
        self.blob_dev_path = Some(blob_dev_path.into());
        self
    }

    pub fn k_p(&mut self, k: usize, p: usize) -> &mut Self {
        self.k_p = Some((k, p));
        self
    }

    pub fn test_load(&mut self, num: usize) -> &mut Self {
        self.test_num = Some(num);
        self
    }

    pub fn slice_size(&mut self, slice_size: usize) -> &mut Self {
        self.slice_size = Some(slice_size);
        self
    }

    pub fn manner(&mut self, manner: Manner) -> &mut Self {
        self.manner = manner;
        self
    }

    pub fn out_dir_path(&mut self, out_dir_path: impl Into<PathBuf>) -> &mut Self {
        self.out_dir_path = Some(out_dir_path.into());
        self
    }

    pub fn run(&self) -> SUResult<()> {
        match self.manner {
            Manner::Baseline => self.baseline(),
            Manner::MergeStripe => self.merge_stripe(),
            Manner::TraceDryRun => self.dryrun(),
        }
    }
}

struct UpdateRequest {
    slice_data: Vec<u8>,
    block_id: BlockId,
    offset: usize,
}
