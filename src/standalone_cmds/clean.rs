use std::{
    io::Write,
    path::{Path, PathBuf},
};

use crate::{standalone_cmds::dev_display, SUResult};

#[derive(Debug, Default)]
pub struct Cleaner {
    ssd_dev_path: Option<PathBuf>,
    hdd_dev_path: Option<PathBuf>,
}

impl Cleaner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ssd_dev_path(&mut self, ssd_dev_path: impl AsRef<std::path::Path>) -> &mut Self {
        self.ssd_dev_path = Some(ssd_dev_path.as_ref().to_path_buf());
        self
    }

    pub fn hdd_dev_path(&mut self, hdd_dev_path: impl AsRef<std::path::Path>) -> &mut Self {
        self.hdd_dev_path = Some(hdd_dev_path.as_ref().to_path_buf());
        self
    }

    pub fn run(&self) -> SUResult<()> {
        fn purge_dir(path: &Path) -> SUResult<()> {
            use std::fs;
            for entry in fs::read_dir(path)? {
                fs::remove_dir_all(entry?.path())?;
            }
            Ok(())
        }
        if self.ssd_dev_path.is_some() {
            let dev = self.ssd_dev_path.as_ref().unwrap();
            print!("purging ssd dev ({})...", dev_display(dev));
            std::io::stdout().flush().unwrap();
            purge_dir(dev)?;
            println!("done");
        }
        if self.hdd_dev_path.is_some() {
            let dev = self.hdd_dev_path.as_ref().unwrap();
            print!("purging hdd dev ({})...", dev_display(dev));
            purge_dir(dev)?;
            println!("done")
        }
        Ok(())
    }
}
