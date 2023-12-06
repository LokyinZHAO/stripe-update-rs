use std::sync::OnceLock;

use crate::SUError;

#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Config {
    ec_k: usize,
    ec_p: usize,
    block_size: usize,
    block_num_per_container: usize,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn init_config(config_file: &std::path::Path) -> crate::SUResult<()> {
    let f = std::fs::File::open(config_file)?;
    CONFIG
        .set(serde_json::from_reader(f).map_err(|e| SUError::other(e.to_string()))?)
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

pub fn block_size() -> usize {
    get_config().block_size
}

pub fn block_num_per_container() -> usize {
    get_config().block_num_per_container
}
