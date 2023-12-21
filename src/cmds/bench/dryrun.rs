use std::{io::Write, num::NonZeroUsize, path::PathBuf};

use indicatif::ProgressIterator;
use rand::Rng;

use crate::{storage::MostModifiedBlockEvict, SUResult};

use super::Bench;

const PLOT_FILE_NAME: &str = "dryrun-trace.svg";

/// Draw a plot named with `dryrun-trace` in `out_path`
///
/// # Parameters
/// - stats: the statistics for this plot, x for slice index, y for value.
/// - total: accumulates of slice value, expected to be `stats.iter().sum()`
///
/// # Return
/// Path to the plot
///
/// # Panics
/// - If `stats.len() != 101`
fn draw_plot(
    stats: &[usize],
    total: usize,
    out_path: &std::path::Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    use plotters::prelude::*;
    // plot config
    const X_DES: &str = "Proportion of updated portion of evicted blocks (%)";
    const Y_DES: &str = "Accumulative # evicted blocks";
    assert!(stats.len() == 101);
    let path = {
        let mut path = out_path.to_owned();
        path.push(PLOT_FILE_NAME);
        path
    };
    let root = SVGBackend::new(path.as_path(), (400, 300)).into_drawing_area();
    root.fill(&WHITE)?;
    let mut chart = ChartBuilder::on(&root)
        .caption(
            "updated proportion of evicted blocks",
            (FontFamily::SansSerif, 20).into_font(),
        )
        .margin(10)
        .x_label_area_size(30)
        .y_label_area_size(50)
        .build_cartesian_2d(0..100_usize, 0..total)?;
    chart
        .configure_mesh()
        .x_desc(X_DES)
        .y_desc(Y_DES)
        .max_light_lines(2)
        .draw()?;
    chart
        .draw_series(LineSeries::new(stats.iter().copied().enumerate(), &RED))
        .unwrap()
        .label("# evicted blocks with the filliness");
    // chart
    //     .configure_series_labels()
    //     .background_style(WHITE.mix(0.8))
    //     .position(SeriesLabelPosition::UpperLeft)
    //     .legend_area_size(20)
    //     .label_font(("sans-serif", 15))
    //     .border_style(BLACK)
    //     .draw()?;

    root.present()?;
    drop(chart);
    drop(root);
    Ok(path)
}

impl Bench {
    pub(super) fn dryrun(&self) -> SUResult<()> {
        let (k, p) = self.k_p.expect("k or p not set");
        let m = k + p;
        let block_size = self.block_size.expect("block size not set");
        let slice_size = self.slice_size.expect("slice size not set");
        let block_num = self.block_num.expect("block num not set");
        let ssd_cap = self.ssd_block_cap.expect("ssd block capacity not set");
        let test_num = self.test_num.expect("test num not set");
        let out_dir_path = self.out_dir_path.to_owned().expect("out dir path not set");
        let ssd_cap_size = ssd_cap * block_size;
        if test_num * slice_size < ssd_cap_size {
            println!("test load is too small to fulfill the ssd capacity");
            return Ok(());
        }

        println!("(k, p): ({k}, {p})");
        println!("block size in bytes: {block_size}");
        println!("block num: {block_num}");
        println!("slice size in bytes: {slice_size}");
        println!("ssd capacity size in bytes: {ssd_cap_size}");
        println!("test load: {test_num}");
        println!("output directory path: {}", out_dir_path.display());

        print!("dry run trace...");
        let mm_evict = MostModifiedBlockEvict::with_max_size(
            NonZeroUsize::new(ssd_cap * block_size).expect("capacity is set to zero"),
        );
        let mut evictions = (0..test_num)
            .progress()
            .with_style(crate::cmds::progress_style_template(Some(
                "dry run trace...",
            )))
            .filter_map(|_| {
                let offset = rand::thread_rng().gen_range(0..(block_size - slice_size));
                let block_id = { (0..).map(|_| rand::thread_rng().gen_range(0..block_num)) }
                    .find(|id| (0..k).contains(&(*id % m)))
                    .unwrap();
                use crate::storage::EvictStrategySlice;
                mm_evict.push(block_id, offset..(offset + slice_size))
            })
            .map(|(_, ranges)| ranges.len())
            .collect::<Vec<_>>();
        let evicted_num = evictions.len();
        evictions.sort();
        assert!(evictions.iter().all(|&size| size <= block_size));
        let mut stats = vec![0_usize; 101];
        evictions.iter().for_each(|&size| {
            let fill = f64::from(size as u32) / f64::from(block_size as u32);
            let fill = (fill * 100f64) as usize;
            assert!((0..=100).contains(&fill));
            stats[fill] += 1;
        });
        println!("done");
        println!("evictions: {}", evicted_num);
        stats
            .iter()
            .enumerate()
            .filter(|(_, &val)| val != 0)
            .for_each(|(idx, val)| println!("\tstats: fill {idx}% #{val}"));
        let average_fill: usize = stats
            .iter()
            .enumerate()
            .map(|(fill, &val)| fill * val)
            .sum::<usize>()
            / evicted_num;
        println!("average fill: {average_fill}");
        let mut acc: usize = 0;
        let accumulate_stat = stats
            .iter()
            .map(|i| {
                acc += i;
                acc
            })
            .collect::<Vec<_>>();
        print!("drawing plot...");
        std::io::stdout().flush().unwrap();

        match draw_plot(&accumulate_stat, evicted_num, &out_dir_path) {
            Ok(path) => {
                println!("done, plot path: {}", path.display());
            }
            Err(e) => {
                eprintln!("fail to draw the plot: {e}")
            }
        };
        Ok(())
    }
}
