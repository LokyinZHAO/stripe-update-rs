fn main() {
    use clap::Parser;
    let args = Cli::parse();
    match args.cmd {
        Commands::BuildData { config, purge } => build_data(&config, purge),
        Commands::Benchmark { config } => benchmark(&config),
    };
}

fn build_data(config_path: &std::path::Path, purge: bool) {
    stripe_update::config::init_config_toml(config_path).unwrap();
    use stripe_update::config;
    stripe_update::data_builder::DataBuilder::new()
        .block_num(config::block_num())
        .block_size(config::block_size())
        .hdd_dev_path(config::hdd_dev_path())
        .ssd_dev_path(config::ssd_dev_path())
        .purge(purge)
        .ssd_block_capacity(config::ssd_block_capacity())
        .k_p(config::ec_k(), config::ec_p())
        .build()
        .unwrap();
}

fn benchmark(_config_path: &std::path::Path) {
    todo!()
}

use clap::Subcommand;

#[derive(Debug, clap::Parser)]
#[command(name = "supg")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Build data
    #[command(arg_required_else_help = true)]
    BuildData {
        #[arg(short, long)]
        config: std::path::PathBuf,
        #[arg(short, long)]
        purge: bool,
    },
    /// Benchmark
    Benchmark {
        #[arg(short, long)]
        config: std::path::PathBuf,
    },
}
