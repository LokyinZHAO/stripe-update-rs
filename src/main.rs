fn main() {
    use clap::Parser;
    let args = Cli::parse();
    match args.cmd {
        Commands::BuildData { config, purge } => build_data(&config, purge),
        Commands::Benchmark { config, manner } => benchmark(&config, manner),
        Commands::Clean { config, ssd, hdd } => cleanup(&config, ssd, hdd),
        Commands::Hitchhiker { config, dev } => hitchhiker(&config, &dev),
    };
}

fn build_data(config_path: &std::path::Path, purge: bool) {
    stripe_update::config::init_config_toml(config_path);
    stripe_update::config::validate_config();
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
        .unwrap_or_else(|e| panic!("fail to benchmark, {e}"));
}

fn benchmark(config_path: &std::path::Path, manner: Manner) {
    use stripe_update::config;
    stripe_update::config::init_config_toml(config_path);
    stripe_update::config::validate_config();
    stripe_update::bench::Bench::new()
        .block_num(config::block_num())
        .block_size(config::block_size())
        .hdd_dev_path(config::hdd_dev_path())
        .ssd_dev_path(config::ssd_dev_path())
        .slice_size(config::slice_size())
        .test_load(config::test_load())
        .ssd_block_capacity(config::ssd_block_capacity())
        .k_p(config::ec_k(), config::ec_p())
        .out_dir_path(config::out_dir_path())
        .manner(manner)
        .run()
        .unwrap_or_else(|e| panic!("fail to benchmark, {e}"));
}

fn cleanup(config_path: &std::path::Path, ssd: bool, hdd: bool) {
    use stripe_update::config;
    stripe_update::config::init_config_toml(config_path);
    stripe_update::config::validate_config();
    let mut cleaner = stripe_update::clean::Cleaner::new();
    if ssd {
        cleaner.ssd_dev_path(config::ssd_dev_path());
    }
    if hdd {
        cleaner.hdd_dev_path(config::hdd_dev_path());
    }
    cleaner
        .run()
        .unwrap_or_else(|e| panic!("fail to benchmark, {e}"));
}

fn hitchhiker(config_path: &std::path::Path, dev_map: &std::path::Path) {
    use stripe_update::config;
    stripe_update::config::init_config_toml(config_path);
    stripe_update::config::validate_config();
    let dev = std::io::BufReader::new(std::fs::File::open(dev_map).unwrap())
        .lines()
        .into_iter()
        .map(Result::unwrap)
        .map(|line| std::path::PathBuf::from(line))
        .collect::<Vec<_>>();
    stripe_update::hitchhiker_bench::HitchhikerBench::new()
        .block_num(config::block_num())
        .block_size(config::block_size())
        .dev_path(dev)
        .test_load(config::test_load())
        .k_p(config::ec_k(), config::ec_p())
        .out_dir_path(config::out_dir_path())
        .run()
        .unwrap_or_else(|e| panic!("fail to benchmark, {e}"));
}

use std::io::BufRead;

use clap::Subcommand;
use stripe_update::bench::Manner;

#[derive(Debug, clap::Parser)]
#[command(name = "supg")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Build data set
    #[command(arg_required_else_help = true)]
    BuildData {
        /// configuration file in toml format
        #[arg(short, long)]
        config: std::path::PathBuf,
        /// purge the existing dev directory
        #[arg(short, long)]
        purge: bool,
    },
    /// Benchmark
    #[command(arg_required_else_help = true)]
    Benchmark {
        /// configuration file in toml format
        #[arg(short, long)]
        config: std::path::PathBuf,
        /// bench mark manners
        #[arg(short, long, default_value_t = Manner::Baseline)]
        manner: Manner,
    },
    /// Hitchhiker run
    #[command(arg_required_else_help = true)]
    Hitchhiker {
        /// configuration file in toml format
        #[arg(short, long)]
        config: std::path::PathBuf,
        /// device path map file
        #[arg(short, long)]
        dev: std::path::PathBuf,
    },
    /// Clean up the dev directory
    #[command(arg_required_else_help = true)]
    Clean {
        /// configuration file in toml format
        #[arg(short, long)]
        config: std::path::PathBuf,
        #[arg(short, long, default_value_t = false)]
        ssd: bool,
        #[arg(short, long, default_value_t = false)]
        hdd: bool,
    },
}
