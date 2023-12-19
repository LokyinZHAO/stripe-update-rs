pub mod bench;
pub mod data_builder;

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
