fn main() {
    use clap::Parser;
    let args = Cli::parse();
    match args.cmd {
        Commands::Coordinator { cmd, config } => launch_coordinator(cmd, config),
        Commands::Worker { config, id } => launch_worker(id.get(), config),
    };
}

use std::{num::NonZeroUsize, path::PathBuf};

use clap::Subcommand;
use stripe_update::{
    cluster,
    config::{self, ec_k},
};

#[derive(Debug, clap::Parser)]
#[command(name = "cluster")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// launch a coordinator to manage the cluster
    #[command(arg_required_else_help = true)]
    Coordinator {
        /// subcommand for coordinator
        #[command(subcommand)]
        cmd: CoordinatorCmds,
        /// configuration file in toml format
        #[arg(short, long)]
        config: std::path::PathBuf,
    },
    /// launch a worker to do requests
    #[command(arg_required_else_help = true)]
    Worker {
        /// configuration file in toml format
        #[arg(short, long)]
        config: std::path::PathBuf,
        /// worker id
        #[arg(short, long)]
        id: NonZeroUsize,
    },
}

#[derive(Debug, Subcommand)]
enum CoordinatorCmds {
    /// Build data for the cluster
    BuildData,
    /// Kill all workers
    KillAll,
}

fn launch_coordinator(cmd: CoordinatorCmds, config: PathBuf) {
    config::init_config_toml(&config);
    config::validate_config();
    config::validate_cluster_config(None);
    let coordinator = crate::cluster::coordinator::CoordinatorBuilder::default()
        .redis_url(config::redis_url().expect("redis url not set in config file"))
        .block_size(NonZeroUsize::new(config::block_size()).unwrap())
        .block_num(NonZeroUsize::new(config::block_num()).unwrap())
        .worker_num(
            NonZeroUsize::new(config::worker_num().expect("worker num not set in config file"))
                .unwrap(),
        )
        .k_p(
            NonZeroUsize::new(ec_k()).unwrap(),
            NonZeroUsize::new(config::ec_p()).unwrap(),
        )
        .build()
        .unwrap_or_else(|e| panic!("FATAL ERROR in coordinator builder: {e}"));
    match cmd {
        CoordinatorCmds::BuildData => coordinator.build_data(),
        CoordinatorCmds::KillAll => coordinator.kill_all(),
    }
    .unwrap_or_else(|e| panic!("FATAL ERROR in coordinator: {e}"))
}

fn launch_worker(id: usize, config: PathBuf) {
    config::init_config_toml(&config);
    config::validate_config();
    config::validate_cluster_config(Some(id));
    cluster::worker::WorkerBuilder::default()
        .id(id)
        .client(config::redis_url().expect("redis url not set in config file"))
        .ssd_dev_path(config::worker_ssd_dev_path(id).expect("ssd dev path not set in config file"))
        .hdd_dev_path(config::worker_hdd_dev_path(id).expect("hdd dev path not set in config file"))
        .block_size(NonZeroUsize::new(config::block_size()).unwrap())
        .work()
        .unwrap_or_else(|e| panic!("FATAL ERROR in worker: {e}"))
}
