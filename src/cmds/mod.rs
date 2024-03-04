pub mod bench;
pub mod clean;
pub mod data_builder;
pub mod hitchhiker_bench;

fn progress_style_template(msg: Option<&str>) -> indicatif::ProgressStyle {
    match msg {
        Some(msg) => indicatif::ProgressStyle::with_template(
            format!("{msg} [{{elapsed_precise}}] {{bar:30.white}} {{pos:>7}}/{{len:7}}").as_str(),
        )
        .unwrap(),
        None => indicatif::ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:30.cyan/blue} {pos:>7}/{len:7}",
        )
        .unwrap(),
    }
}

fn dev_display(dev: &std::path::Path) -> String {
    let mut display = dev.display().to_string();
    if dev.is_symlink() {
        display += format!(" -> {}", std::fs::read_link(dev).unwrap().display()).as_str();
    }
    display
}
