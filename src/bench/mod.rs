use std::path::{Path, PathBuf};

use crate::SUResult;

mod baseline;
mod dryrun;

#[derive(Debug, Default, serde::Deserialize, Clone, clap::ValueEnum)]
pub enum Manner {
    /// No optimization, ssd fetches and updates in block unit.
    #[default]
    Baseline,
    /// No disk write/read is performed, only generate and report disk access trace.
    TraceDryRun,
}

impl std::fmt::Display for Manner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Manner::Baseline => f.write_str("baseline"),
            Manner::TraceDryRun => f.write_str("trace_dryrun"),
        }
    }
}

#[derive(Debug, Default)]
pub struct Bench {
    block_size: Option<usize>,
    block_num: Option<usize>,
    ssd_cap: Option<usize>,
    ssd_dev_path: Option<PathBuf>,
    hdd_dev_path: Option<PathBuf>,
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
        self.ssd_cap = Some(ssd_block_capacity);
        self
    }

    pub fn ssd_dev_path(&mut self, ssd_dev_path: impl Into<PathBuf>) -> &mut Self {
        self.ssd_dev_path = Some(ssd_dev_path.into());
        self
    }

    pub fn hdd_dev_path(&mut self, hdd_dev_path: impl Into<PathBuf>) -> &mut Self {
        self.hdd_dev_path = Some(hdd_dev_path.into());
        self
    }

    pub fn k_p(&mut self, k: usize, p: usize) -> &mut Self {
        self.k_p = Some((k, p));
        self
    }

    pub fn test_num(&mut self, num: usize) -> &mut Self {
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
            Manner::TraceDryRun => self.dryrun(),
        }
    }
}

fn dev_display(dev: &Path) -> String {
    let mut display = dev.display().to_string();
    if dev.is_symlink() {
        display += format!(" -> {}", std::fs::read_link(dev).unwrap().display()).as_str();
    }
    display
}
