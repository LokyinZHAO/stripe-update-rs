use std::{
    io::Write,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use bytes::BytesMut;

use crate::{
    erasure_code::{Block, ErasureCode, PartialStripe, ReedSolomon},
    storage::{BlockId, BlockStorage, HDDStorage, SSDStorage, SliceStorage},
    SUResult,
};

#[derive(Debug, Default)]
pub struct Bench {
    block_size: Option<usize>,
    block_num: Option<usize>,
    ssd_cap: Option<usize>,
    ssd_dev_path: Option<PathBuf>,
    hdd_dev_path: Option<PathBuf>,
    k_p: Option<(usize, usize)>,
    test_num: Option<usize>,
    slice_size: Option<usize>,
}

impl Bench {
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

    pub fn k_p(&mut self, k: usize, p: usize) -> &mut Self {
        self.k_p = Some((k, p));
        self
    }

    pub fn test_num(&mut self, num: usize) -> &mut Self {
        self.test_num = Some(num);
        self
    }

    pub fn slice_size(&mut self, slice_size: usize) -> &mut Self {
        self.slice_size = Some(slice_size);
        self
    }

    pub fn run(&self) -> SUResult<()> {
        const CHANNEL_SIZE: usize = 1024;
        let (update_producer, update_consumer) =
            std::sync::mpsc::sync_channel::<UpdateItem>(CHANNEL_SIZE);
        let (k, p) = self.k_p.expect("k or p not set");
        let m = k + p;
        let block_size = self.block_size.expect("block size not set");
        let slice_size = self.slice_size.expect("slice size not set");
        let hdd_dev_path = self.hdd_dev_path.clone().expect("hdd dev path not set");
        let ssd_dev_path = self.ssd_dev_path.clone().expect("ssd dev path not set");
        let block_num = self.block_num.expect("block num not set");
        let ssd_cap = self.ssd_cap.expect("ssd block capacity not set");
        let test_num = self.test_num.expect("test num not set");
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
        println!("hdd dev path: {hdd_dev_display}");
        println!("ssd dev path: {ssd_dev_display}");
        println!("ssd block capacity: {ssd_cap}");
        println!("slice size: {slice_size}");
        println!("test num: {test_num}");
        print!("benchmark start...");
        std::io::stdout().flush().unwrap();
        // data generator
        let generator_handle = std::thread::spawn(move || {
            use rand::Rng;
            (0..test_num).for_each(|_| {
                let offset = rand::thread_rng().gen_range(0..(block_size - slice_size));
                let block_id = { (0..).map(|_| rand::thread_rng().gen_range(0..block_num)) }
                    .find(|id| (0..k).contains(&(*id % m)))
                    .unwrap();
                let slice_data = rand::thread_rng()
                    .sample_iter(rand::distributions::Standard)
                    .take(slice_size)
                    .collect::<Vec<_>>();
                update_producer
                    .send(UpdateItem {
                        slice_data,
                        block_id,
                        offset,
                    })
                    .unwrap();
            });
        });
        // data encoder
        let encoder_handle = std::thread::spawn(move || {
            let ec =
                ReedSolomon::from_k_p(NonZeroUsize::new(k).unwrap(), NonZeroUsize::new(p).unwrap());
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
            let mut duration = std::time::Duration::ZERO;
            let mut cnt = 0_usize;
            while let Ok(UpdateItem {
                slice_data,
                block_id,
                offset,
            }) = update_consumer.recv()
            {
                let epoch = std::time::Instant::now();
                let mut buf = BytesMut::zeroed(block_size * (1 + p));
                let mut source = buf.split_to(block_size);
                ssd_storage
                    .get_block(block_id, &mut source)
                    .unwrap()
                    .unwrap_or_else(|| panic!("block {block_id} not found"));
                let source = Block::from(source);
                let parity = (k..m)
                    .map(|i| {
                        let id = block_id - block_id % m + i;
                        let mut parity = buf.split_to(block_size);
                        ssd_storage.get_block(id, &mut parity).unwrap().unwrap();
                        Block::from(parity)
                    })
                    .collect::<Vec<_>>();
                let mut partial_stripe = PartialStripe::make_absent_from_k_p(
                    NonZeroUsize::new(k).unwrap(),
                    NonZeroUsize::new(p).unwrap(),
                    NonZeroUsize::new(block_size).unwrap(),
                );
                partial_stripe.replace_block(block_id % m, Some(source));
                parity.into_iter().zip(k..m).for_each(|(parity, idx)| {
                    partial_stripe.replace_block(idx, Some(parity));
                });
                ec.delta_update(&slice_data, block_id % m, offset, &mut partial_stripe)
                    .unwrap();
                partial_stripe.iter_present().for_each(|(id, block)| {
                    let id = block_id - block_id % m + id;
                    ssd_storage
                        .put_slice(id, offset, &block[offset..offset + slice_data.len()])
                        .unwrap()
                        .unwrap();
                });
                let elapsed = epoch.elapsed();
                duration += elapsed;
                cnt += 1;
            }
            (duration, cnt)
        });
        generator_handle.join().unwrap();
        let (duration, cnt) = encoder_handle.join().unwrap();
        println!("done");
        println!(
            "benchmarked {test_num} updates request in {}s{}ms",
            duration.as_secs(),
            duration.as_millis()
        );
        println!(
            "IOPS: {}",
            cnt / usize::try_from(duration.as_secs()).unwrap()
        );
        Ok(())
    }
}

struct UpdateItem {
    slice_data: Vec<u8>,
    block_id: BlockId,
    offset: usize,
}
