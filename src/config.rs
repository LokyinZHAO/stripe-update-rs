use std::{io::Read, sync::OnceLock};

use crate::SUError;

#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Config {
    ec_k: usize,
    ec_p: usize,
    block_size: usize,
    block_num: usize,
    ssd_block_capacity: usize,
    ssd_dev_path: std::path::PathBuf,
    hdd_dev_path: std::path::PathBuf,
    test_num: usize,
    slice_size: usize,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn init_config_toml(config_file: &std::path::Path) -> crate::SUResult<()> {
    let mut config_str = String::new();
    std::fs::File::open(config_file)?.read_to_string(&mut config_str)?;
    CONFIG
        .set(toml::from_str(&config_str).map_err(|e| SUError::other(e.to_string()))?)
        .expect("initialize config more than once");
    Ok(())
}

fn get_config() -> &'static Config {
    CONFIG.get().unwrap()
}

pub fn ec_k() -> usize {
    get_config().ec_k
}

pub fn ec_p() -> usize {
    get_config().ec_p
}

pub fn ec_m() -> usize {
    ec_k() + ec_p()
}

pub fn hdd_dev_path() -> std::path::PathBuf {
    get_config().hdd_dev_path.clone()
}

pub fn ssd_dev_path() -> std::path::PathBuf {
    get_config().ssd_dev_path.clone()
}

pub fn ssd_block_capacity() -> usize {
    get_config().ssd_block_capacity
}

pub fn block_size() -> usize {
    get_config().block_size
}

pub fn block_num() -> usize {
    get_config().block_num
}

pub fn test_num() -> usize {
    get_config().test_num
}

pub fn slice_size() -> usize {
    get_config().slice_size
}
