use std::{io::Read, sync::OnceLock};

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
    out_dir_path: std::path::PathBuf,
    test_num: usize,
    slice_size: usize,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

/// Initialize configuration with toml file, and panic if any error occurs.
pub fn init_config_toml(config_file: &std::path::Path) {
    let mut config_str = String::new();
    std::fs::File::open(config_file)
        .unwrap_or_else(|e| panic!("fail to open the config file: {e}"))
        .read_to_string(&mut config_str)
        .unwrap_or_else(|e| panic!("fail to read the config file: {e}"));
    CONFIG
        .set(
            toml::from_str(&config_str)
                .unwrap_or_else(|e| panic!("fail to parse the config file: {e}")),
        )
        .expect("initialize config more than once");
}

/// Validate the configuration, and panic if any configuration is illegal.
pub fn validate_config() {
    let config = CONFIG.get().expect("config not initialized");
    if !config.hdd_dev_path.is_dir() {
        panic!(
            "hdd dev path {} is not a directory",
            config.hdd_dev_path.display()
        );
    }
    if !config.ssd_dev_path.is_dir() {
        panic!(
            "ssd dev path {} is not a directory",
            config.ssd_dev_path.display()
        );
    }
    if !config.out_dir_path.is_dir() {
        panic!(
            "output path {} is not a directory",
            config.out_dir_path.display()
        );
    }
    if config.slice_size > config.block_size {
        panic!(
            "slice size {} is greater than block size {}",
            config.slice_size, config.block_size
        );
    }
}

fn get_config() -> &'static Config {
    CONFIG.get().expect("config not initialized")
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

pub fn out_dir_path() -> std::path::PathBuf {
    get_config().out_dir_path.clone()
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
