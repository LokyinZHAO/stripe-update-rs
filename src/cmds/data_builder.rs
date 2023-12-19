use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use indicatif::ProgressIterator;

use crate::{
    erasure_code::{ErasureCode, ReedSolomon, Stripe},
    storage::{BlockStorage, HDDStorage, SSDStorage},
    SUResult,
};

#[derive(Debug, Default)]
pub struct DataBuilder {
    block_size: Option<usize>,
    block_num: Option<usize>,
    ssd_cap: Option<usize>,
    ssd_dev_path: Option<PathBuf>,
    hdd_dev_path: Option<PathBuf>,
    purge: bool,
    k_p: Option<(usize, usize)>,
}

impl DataBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn block_size(&mut self, block_size: usize) -> &mut Self {
        self.block_size = Some(block_size);
        self
    }

    pub fn block_num(&mut self, block_num: usize) -> &mut Self {
        self.block_num = Some(block_num);
        self
    }

    pub fn ssd_block_capacity(&mut self, ssd_block_capacity: usize) -> &mut Self {
        self.ssd_cap = Some(ssd_block_capacity);
        self
    }

    pub fn ssd_dev_path(&mut self, ssd_dev_path: impl AsRef<std::path::Path>) -> &mut Self {
        self.ssd_dev_path = Some(ssd_dev_path.as_ref().to_path_buf());
        self
    }

    pub fn hdd_dev_path(&mut self, hdd_dev_path: impl AsRef<std::path::Path>) -> &mut Self {
        self.hdd_dev_path = Some(hdd_dev_path.as_ref().to_path_buf());
        self
    }

    pub fn purge(&mut self, purge: bool) -> &mut Self {
        self.purge = purge;
        self
    }

    pub fn k_p(&mut self, k: usize, p: usize) -> &mut Self {
        self.k_p = Some((k, p));
        self
    }

    pub fn build(&self) -> SUResult<()> {
        const CHANNEL_SIZE: usize = 1024;
        let (source_stripe_producer, source_stripe_consumer) =
            std::sync::mpsc::sync_channel::<StripeItem>(CHANNEL_SIZE);
        let (encoded_stripe_producer, encoded_stripe_consumer) =
            std::sync::mpsc::sync_channel::<StripeItem>(CHANNEL_SIZE);
        let (k, p) = self.k_p.expect("k or p not set");
        let m = k + p;
        let block_num = self.block_num.expect("block num not set");
        let stripe_num = block_num / m;
        let block_size = self.block_size.expect("block size not set");
        let hdd_dev_path = self.hdd_dev_path.clone().expect("hdd dev path not set");
        let ssd_dev_path = self.ssd_dev_path.clone().expect("ssd dev path not set");
        let ssd_cap = self.ssd_cap.expect("ssd block capacity not set");
        fn dev_display(dev: &Path) -> String {
            let mut display = dev.display().to_string();
            if dev.is_symlink() {
                display += format!(" -> {}", std::fs::read_link(dev).unwrap().display()).as_str();
            }
            display
        }
        let ssd_dev_display = dev_display(&ssd_dev_path);
        let hdd_dev_display = dev_display(&hdd_dev_path);
        println!("RS({m}, {k})");
        println!("block size: {block_size}");
        println!("block num: {block_num}");
        println!("stripe num: {stripe_num}");
        println!("ssd block capacity: {ssd_cap}");
        println!("hdd dev path: {hdd_dev_display}");
        println!("ssd dev path: {ssd_dev_display}");
        if self.purge {
            print!("purging dir...");
            fn purge_dir(path: &Path) -> SUResult<()> {
                use std::fs;
                for entry in fs::read_dir(path)? {
                    fs::remove_dir_all(entry?.path())?;
                }
                Ok(())
            }
            purge_dir(hdd_dev_path.as_path())?;
            purge_dir(ssd_dev_path.as_path())?;
            println!("done")
        }
        let epoch = std::time::Instant::now();
        // data generator
        let generator_handle = std::thread::spawn(move || {
            use rand::Rng;
            (0..stripe_num).for_each(|stripe_id| {
                let mut stripe = Stripe::zero(
                    NonZeroUsize::new(k).unwrap(),
                    NonZeroUsize::new(p).unwrap(),
                    NonZeroUsize::new(block_size).unwrap(),
                );
                stripe.iter_mut_source().for_each(|source_block| {
                    source_block
                        .iter_mut()
                        .for_each(|b| *b = rand::thread_rng().gen())
                });
                let block_id_range = (stripe_id * m)..(stripe_id * m + m);
                source_stripe_producer
                    .send(StripeItem {
                        stripe,
                        block_id_range,
                    })
                    .unwrap();
            });
        });
        // data encoder
        let encoder_handle = std::thread::spawn(move || {
            let ec =
                ReedSolomon::from_k_p(NonZeroUsize::new(k).unwrap(), NonZeroUsize::new(p).unwrap());
            while let Ok(StripeItem {
                mut stripe,
                block_id_range,
            }) = source_stripe_consumer.recv()
            {
                ec.encode_stripe(&mut stripe).unwrap();
                encoded_stripe_producer
                    .send(StripeItem {
                        stripe,
                        block_id_range,
                    })
                    .unwrap();
            }
        });
        // data store
        let store_handle = std::thread::spawn(move || {
            let hdd_storage =
                HDDStorage::connect_to_dev(hdd_dev_path, NonZeroUsize::new(block_size).unwrap())
                    .unwrap();
            let ssd_storage = SSDStorage::connect_to_dev(
                ssd_dev_path,
                NonZeroUsize::new(block_size).unwrap(),
                NonZeroUsize::new(ssd_cap).unwrap(),
                hdd_storage,
            )
            .unwrap();
            (0..stripe_num)
                .map(|_| {
                    encoded_stripe_consumer
                        .recv()
                        .expect("fail to recv a stripe to store")
                })
                .progress_with_style(super::progress_style_template(Some("building data...")))
                .for_each(
                    |StripeItem {
                         stripe,
                         block_id_range,
                     }| {
                        assert_eq!(block_id_range.len(), stripe.m());
                        stripe
                            .iter_source()
                            .chain(stripe.iter_parity())
                            .zip(block_id_range)
                            .for_each(|(block, id)| ssd_storage.put_block(id, block).unwrap());
                    },
                );
            assert!(encoded_stripe_consumer.recv().is_err());
            println!("building data...done");
        });
        generator_handle.join().unwrap();
        encoder_handle.join().unwrap();
        store_handle.join().unwrap();
        let elapsed = epoch.elapsed();
        println!(
            "built {block_num} blocks in {}s{}ms",
            elapsed.as_secs(),
            elapsed.as_millis()
        );
        println!(
            "throughput: {} blocks/s",
            block_num / usize::try_from(elapsed.as_secs()).unwrap()
        );
        Ok(())
    }
}

struct StripeItem {
    stripe: Stripe,
    block_id_range: std::ops::Range<usize>,
}
