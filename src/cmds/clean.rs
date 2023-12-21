use std::path::PathBuf;

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
}
