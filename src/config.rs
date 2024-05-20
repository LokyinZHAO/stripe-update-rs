use std::{io::Read, num::NonZeroUsize, sync::OnceLock};

use bytesize::ByteSize;

#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Config {
    ec_k: usize,
    ec_p: usize,
    block_size: ByteSize,
    block_num: usize,
    ssd_block_capacity: usize,
    out_dir_path: std::path::PathBuf,
    test_num: usize,
    slice_size: ByteSize,
    standalone: Option<StandaloneConfig>,
    cluster: Option<ClusterConfig>,
}

#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct StandaloneConfig {
    ssd_dev_path: std::path::PathBuf,
    blob_dev_path: std::path::PathBuf,
}

#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ClusterConfig {
    redis_url: String,
    worker_num: NonZeroUsize,
    workers: Vec<WorkerConfig>,
}

#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct WorkerConfig {
    ssd_dev_path: std::path::PathBuf,
    blob_dev_path: std::path::PathBuf,
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

/// Validate the general configuration, and panic if any configuration is illegal.
///
/// To validate the standalone configuration, use `validate_standalone_config`.
/// To validate the cluster configuration, use `validate_cluster_config`.
pub fn validate_config() {
    let config = CONFIG.get().expect("config not initialized");
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

/// Validate the standalone configuration, and panic if any configuration is illegal.
///
/// This function must be called after `validate_config`.
pub fn validate_standalone_config() {
    let config = CONFIG.get().expect("config not initialized");
    let config = config
        .standalone
        .as_ref()
        .expect("standalone config not set");
    if !config.blob_dev_path.is_dir() {
        panic!(
            "blob dev path {} is not a directory",
            config.blob_dev_path.display()
        );
    }
    if !config.ssd_dev_path.is_dir() {
        panic!(
            "ssd dev path {} is not a directory",
            config.ssd_dev_path.display()
        );
    }
}

/// Validate the cluster configuration, and panic if any configuration is illegal
///
/// This function must be called after `validate_config`.
///
/// # Arguments
/// - worker_id: the worker id to validate, and `None` stands for coordinator
pub fn validate_cluster_config(worker_id: Option<usize>) {
    let config = CONFIG.get().expect("config not initialized");
    let cluster = config.cluster.as_ref().expect("cluster config not set");
    if cluster.worker_num.get() < 1 {
        panic!("worker num must be greater than 0");
    }
    if cluster.worker_num.get() > cluster.workers.len() {
        panic!("worker num must be equal to the number of worker dev path");
    }
    if let Some(worker_id) = worker_id {
        if worker_id == 0 || worker_id > cluster.worker_num.get() {
            panic!("worker id ranges from 0 to {}", cluster.worker_num.get());
        }
        let worker = &cluster.workers[worker_id - 1];
        if !worker.ssd_dev_path.is_dir() {
            panic!(
                "worker {} ssd dev path {} is not a directory",
                worker_id,
                worker.ssd_dev_path.display()
            );
        }
        if !worker.blob_dev_path.is_dir() {
            panic!(
                "worker {} blob dev path {} is not a directory",
                worker_id,
                worker.blob_dev_path.display()
            );
        }
    }
}

/// Get the configuration, panic if not initialized.
fn get_config() -> &'static Config {
    CONFIG.get().expect("config not initialized")
}

/// Get `k` of erasure code
pub fn ec_k() -> usize {
    get_config().ec_k
}

/// Get `p` of erasure code
pub fn ec_p() -> usize {
    get_config().ec_p
}

/// Get `m` of erasure code
pub fn ec_m() -> usize {
    ec_k() + ec_p()
}

/// Get path to the blob device, expected to be a directory linked to a HDD device
pub fn blob_dev_path() -> std::path::PathBuf {
    get_config()
        .standalone
        .as_ref()
        .expect("standalone config not set")
        .blob_dev_path
        .clone()
}

/// Get path to the ssd device, expected to be a directory
pub fn ssd_dev_path() -> std::path::PathBuf {
    get_config()
        .standalone
        .as_ref()
        .expect("standalone config not set")
        .ssd_dev_path
        .clone()
}

/// Get path to the output directory
pub fn out_dir_path() -> std::path::PathBuf {
    get_config().out_dir_path.clone()
}

/// Get the number of block capacity for ssd
pub fn ssd_block_capacity() -> usize {
    get_config().ssd_block_capacity
}

/// Get the size of a block
pub fn block_size() -> usize {
    get_config().block_size.as_u64().try_into().unwrap()
}

/// Get the maximum number of blocks
pub fn block_num() -> usize {
    get_config().block_num
}

/// Get the number of test load
pub fn test_load() -> usize {
    get_config().test_num
}

/// Get the size of a update slice
pub fn slice_size() -> usize {
    get_config().slice_size.as_u64().try_into().unwrap()
}

/// Get the url to connect to redis
pub fn redis_url() -> Option<String> {
    get_config().cluster.as_ref().map(|c| c.redis_url.clone())
}

/// Get the number of workers
pub fn worker_num() -> Option<usize> {
    get_config().cluster.as_ref().map(|c| c.worker_num.get())
}

/// Get the ssd device path of a worker
pub fn worker_ssd_dev_path(worker_id: usize) -> Option<std::path::PathBuf> {
    get_config()
        .cluster
        .as_ref()
        .and_then(|c| c.workers.get(worker_id - 1).map(|w| w.ssd_dev_path.clone()))
}

/// Get the blob device path of a worker
pub fn worker_blob_dev_path(worker_id: usize) -> Option<std::path::PathBuf> {
    get_config().cluster.as_ref().and_then(|c| {
        c.workers
            .get(worker_id - 1)
            .map(|w| w.blob_dev_path.clone())
    })
}

/// Get the interval of heartbeat
pub fn heartbeat_interval() -> std::time::Duration {
    std::time::Duration::from_millis(300)
}
