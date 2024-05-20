fn main() {
    use clap::Parser;
    env_logger::init();
    let args = Cli::parse();
    match args.cmd {
        Commands::Coordinator { cmd, config } => launch_coordinator(cmd, config),
        Commands::Worker { config, id } => launch_worker(id.get(), config),
    };
}

use std::{num::NonZeroUsize, path::PathBuf};

use clap::Subcommand;
use stripe_update::{
    cluster::{self},
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
    /// Purge all the existing data in the cluster
    Purge,
    /// Benchmark stripe update in baseline manner
    BenchmarkBaseline,
    /// Benchmark stripe update in merge manner
    BenchmarkMerge,
    /// Kill all workers
    KillAll,
}

fn launch_coordinator(cmd: CoordinatorCmds, config: PathBuf) {
    config::init_config_toml(&config);
    config::validate_config();
    config::validate_cluster_config(None);
    let builder = crate::cluster::coordinator::CoordinatorBuilder::default()
        .redis_url(config::redis_url().expect("redis url not set in config file"))
        .block_size(NonZeroUsize::new(config::block_size()).unwrap())
        .block_num(NonZeroUsize::new(config::block_num()).unwrap())
        .worker_num(
            NonZeroUsize::new(config::worker_num().expect("worker num not set in config file"))
                .unwrap(),
        )
        .slice_size(NonZeroUsize::new(config::slice_size()).unwrap())
        .test_load(NonZeroUsize::new(config::test_load()).unwrap())
        .buf_capacity(NonZeroUsize::new(config::ssd_block_capacity()).unwrap())
        .k_p(
            NonZeroUsize::new(ec_k()).unwrap(),
            NonZeroUsize::new(config::ec_p()).unwrap(),
        );

    use stripe_update::cluster::coordinator::cmds::*;
    use stripe_update::cluster::coordinator::CoordinatorCmds as Cmds;
    match cmd {
        CoordinatorCmds::BuildData => BuildData::try_from(builder)
            .map(Box::new)
            .and_then(Cmds::exec),
        CoordinatorCmds::BenchmarkMerge => BenchUpdate::try_from(builder.benchmark_merge())
            .map(Box::new)
            .and_then(Cmds::exec),
        CoordinatorCmds::BenchmarkBaseline => BenchUpdate::try_from(builder.benchmark_baseline())
            .map(Box::new)
            .and_then(Cmds::exec),
        CoordinatorCmds::KillAll => KillAll::try_from(builder)
            .map(Box::new)
            .and_then(Cmds::exec),
        CoordinatorCmds::Purge => Purge::try_from(builder).map(Box::new).and_then(Cmds::exec),
    }
    .unwrap_or_else(|e| panic!("FATAL ERROR in coordinator: {e}"));
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
