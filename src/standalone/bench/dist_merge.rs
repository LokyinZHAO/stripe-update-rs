use std::num::NonZeroUsize;

use crate::{
    bench::UpdateRequest, erasure_code::ReedSolomon, standalone_cmds::dev_display, SUResult,
};

use super::Bench;

impl Bench {
    pub(super) fn dist_merge(&self) -> SUResult<()> {
        const CHANNEL_SIZE: usize = 64;
        struct Ack();
        let sync_channel = std::sync::mpsc::sync_channel::<UpdateRequest>(CHANNEL_SIZE);
        let (update_producer, update_consumer) = sync_channel;
        let (k, p) = self.k_p.expect("k or p not set");
        let m = k + p;
        let block_size = self.block_size.expect("block size not set");
        let slice_size = self.slice_size.expect("slice size not set");
        let hdd_dev_path = self.hdd_dev_path.clone().expect("hdd dev path not set");
        let ssd_dev_path = self.ssd_dev_path.clone().expect("ssd dev path not set");
        let block_num = self.block_num.expect("block num not set");
        let ssd_block_cap = self.ssd_block_cap.expect("ssd block capacity not set");
        let ssd_cap = ssd_block_cap * block_size;
        let test_load = self.test_num.expect("test num not set");
        let ssd_dev_display = dev_display(&ssd_dev_path);
        let hdd_dev_display = dev_display(&hdd_dev_path);
        if ssd_dev_path.read_dir().unwrap().next().is_some() {
            panic!("ssd dev path: {ssd_dev_display} is not empty");
        }
        println!("RS({m}, {k})");
        println!("block size: {block_size}");
        println!("block num: {block_num}");
        println!("hdd dev path: {hdd_dev_display}");
        println!("ssd dev path: {ssd_dev_display}");
        println!("ssd block capacity: {ssd_cap}");
        println!("slice size: {slice_size}");
        println!("test num: {test_load}");

        // data generator
        let data_generator_handle = std::thread::spawn(move || {
            use rand::Rng;
            const SEG_SIZE: usize = 4 << 10;
            let seg_num = block_size / SEG_SIZE;
            (0..test_load).for_each(|_| {
                let offset = rand::thread_rng().gen_range(0..seg_num);
                let offset = offset * SEG_SIZE;
                let block_id = { (0..).map(|_| rand::thread_rng().gen_range(0..block_num)) }
                    .find(|id| (0..k).contains(&(*id % m)))
                    .unwrap();
                let slice_data = rand::thread_rng()
                    .sample_iter(rand::distributions::Standard)
                    .take(slice_size)
                    .collect::<Vec<_>>();
                debug_assert!(offset + slice_data.len() <= block_size);
                update_producer
                    .send(UpdateRequest {
                        slice_data,
                        block_id,
                        offset,
                    })
                    .unwrap();
            });
        });

        let encoder_handle = std::thread::spawn(move || {
            let ec =
                ReedSolomon::from_k_p(NonZeroUsize::new(k).unwrap(), NonZeroUsize::new(p).unwrap());

            let mut duration = std::time::Duration::ZERO;
            let mut cnt = 0_usize;

            while let Ok(UpdateRequest {
                slice_data,
                block_id,
                offset,
            }) = update_consumer.recv()
            {
                let epoch = std::time::Instant::now();

                let elapsed = epoch.elapsed();
                duration += elapsed;
                cnt += 1;
            }
            (duration, cnt)
        });

        data_generator_handle.join().unwrap();
        println!("benchmark baseline...done");
        todo!()
    }
}
