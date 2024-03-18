use std::{
    io::Write,
    num::NonZeroUsize,
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::BytesMut;
use indicatif::ProgressIterator;

use crate::{
    bench::UpdateRequest,
    erasure_code::{Block, ErasureCode, PartialStripe, ReedSolomon},
    standalone_cmds::dev_display,
    storage::{
        BlockId, BlockStorage, BufferEviction, FixedSizeSliceBuf, HDDStorage, PartialBlock,
        SSDStorage, SliceBuffer, SliceOpt, SliceStorage,
    },
    SUResult,
};

use super::Bench;

struct UpdateCtx<E: ErasureCode> {
    hdd_storage: HDDStorage,
    block_size: usize,
    ec: E,
}

fn do_update<E: ErasureCode>(
    UpdateCtx {
        hdd_storage,
        block_size,
        ec,
    }: &UpdateCtx<E>,
    block_id: BlockId,
    update_slices: Vec<SliceOpt>,
) {
    let k = ec.k();
    let block_size = *block_size;
    let p = ec.p();
    let m = ec.m();
    let mut buf = BytesMut::zeroed(block_size * (1 + p));
    let mut original_source = buf.split_to(block_size);
    hdd_storage
        .get_block(block_id, &mut original_source)
        .unwrap()
        .unwrap_or_else(|| panic!("block {block_id} not found"));
    let mut source_offset: usize = 0;
    let mut update_source = BytesMut::zeroed(block_size);
    update_slices.iter().for_each(|slice| match slice {
        crate::storage::SliceOpt::Present(data) => {
            update_source[source_offset..source_offset + data.len()].copy_from_slice(data);
            source_offset += data.len();
        }
        crate::storage::SliceOpt::Absent(size) => {
            let range = source_offset..source_offset + size;
            update_source[range.clone()].copy_from_slice(&original_source[range]);
            source_offset += size;
        }
    });
    let source = Block::from(original_source);
    let parity = (k..m)
        .map(|i| {
            let id = block_id - block_id % m + i;
            let mut parity = buf.split_to(block_size);
            hdd_storage.get_block(id, &mut parity).unwrap().unwrap();
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
    ec.delta_update(&update_source, block_id % m, 0, &mut partial_stripe)
        .unwrap();
    partial_stripe.iter_present().for_each(|(id, block)| {
        let id = block_id - block_id % m + id;
        hdd_storage.put_block(id, block).unwrap();
    });
}

impl Bench {
    pub(super) fn baseline(&self) -> SUResult<()> {
        const CHANNEL_SIZE: usize = 64;
        struct Ack();
        let (update_producer, update_consumer) =
            std::sync::mpsc::sync_channel::<UpdateRequest>(CHANNEL_SIZE);
        let (ack_producer, ack_consumer) = std::sync::mpsc::sync_channel::<Ack>(CHANNEL_SIZE);
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
        println!("ssd block capacity: {ssd_block_cap}");
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
        let buffer_len_monitor = Arc::new(AtomicUsize::new(0));
        let buffer_len_updater = Arc::clone(&buffer_len_monitor);
        let encoder_handle = std::thread::spawn(move || {
            let ec =
                ReedSolomon::from_k_p(NonZeroUsize::new(k).unwrap(), NonZeroUsize::new(p).unwrap());
            let hdd_storage =
                HDDStorage::connect_to_dev(hdd_dev_path, NonZeroUsize::new(block_size).unwrap())
                    .unwrap();
            let ssd_storage = FixedSizeSliceBuf::connect_to_dev(
                ssd_dev_path,
                NonZeroUsize::new(block_size).unwrap(),
                NonZeroUsize::new(ssd_cap).unwrap(),
            )
            .unwrap();
            let mut duration = std::time::Duration::ZERO;
            let mut cnt = 0_usize;
            let update_ctx = UpdateCtx::<ReedSolomon> {
                hdd_storage,
                block_size,
                ec,
            };
            while let Ok(UpdateRequest {
                slice_data,
                block_id,
                offset,
            }) = update_consumer.recv()
            {
                let epoch = std::time::Instant::now();
                let evict = ssd_storage
                    .push_slice(block_id, offset, slice_data.as_slice())
                    .unwrap();
                if let Some(BufferEviction {
                    block_id,
                    data: PartialBlock { size, slices },
                }) = evict
                {
                    debug_assert_eq!(size, block_size);
                    do_update(&update_ctx, block_id, slices);
                };
                let elapsed = epoch.elapsed();
                duration += elapsed;
                cnt += 1;
                ack_producer.send(Ack()).unwrap();
            }
            buffer_len_updater.store(0, std::sync::atomic::Ordering::SeqCst);
            while let Some(BufferEviction {
                block_id,
                data: PartialBlock { size, slices },
            }) = ssd_storage.pop()
            {
                let epoch = std::time::Instant::now();
                debug_assert_eq!(size, block_size);
                do_update(&update_ctx, block_id, slices);
                duration += epoch.elapsed();
                cnt += 1;
                ack_producer.send(Ack()).unwrap();
                buffer_len_updater.store(
                    ssd_cap - ssd_storage.len(),
                    std::sync::atomic::Ordering::SeqCst,
                );
            }
            (duration, cnt)
        });

        std::thread::spawn(move || {
            (0..test_load)
                .progress_with_style(crate::standalone_cmds::progress_style_template(Some(
                    "benchmark baseline...",
                )))
                .for_each(|_| {
                    ack_consumer.recv().unwrap();
                });
            std::io::stdout().flush().unwrap();
            let bar = indicatif::ProgressBar::new(ssd_cap.try_into().unwrap());
            bar.set_style(crate::standalone_cmds::progress_style_template(Some(
                "clean up updates buffered in ssd...",
            )));
            while let Ok(_ack) = ack_consumer.recv() {
                bar.set_position(
                    buffer_len_monitor
                        .load(std::sync::atomic::Ordering::SeqCst)
                        .try_into()
                        .unwrap(),
                );
            }
            println!("clean up updates buffered in ssd...done");
        })
        .join()
        .unwrap();
        data_generator_handle.join().unwrap();
        let (duration, cnt) = encoder_handle.join().unwrap();
        println!("benchmark baseline...done");
        println!(
            "benchmarked {test_load} updates request in {}s{}ms",
            duration.as_secs(),
            duration.as_millis()
        );
        println!(
            "OPS: {}",
            cnt * 1000 * 1000 / usize::try_from(duration.as_micros()).unwrap()
        );
        Ok(())
    }

    fn _legacy_baseline(&self) -> SUResult<()> {
        const CHANNEL_SIZE: usize = 1024;
        let (update_producer, update_consumer) =
            std::sync::mpsc::sync_channel::<UpdateRequest>(CHANNEL_SIZE);
        let (k, p) = self.k_p.expect("k or p not set");
        let m = k + p;
        let block_size = self.block_size.expect("block size not set");
        let slice_size = self.slice_size.expect("slice size not set");
        let hdd_dev_path = self.hdd_dev_path.clone().expect("hdd dev path not set");
        let ssd_dev_path = self.ssd_dev_path.clone().expect("ssd dev path not set");
        let block_num = self.block_num.expect("block num not set");
        let ssd_cap = self.ssd_block_cap.expect("ssd block capacity not set");
        let test_num = self.test_num.expect("test num not set");
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
                    .send(UpdateRequest {
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
            while let Ok(UpdateRequest {
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
            "OPS: {}",
            cnt / usize::try_from(duration.as_secs()).unwrap()
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use bytes::BytesMut;

    use crate::{
        bench::{baseline::do_update, UpdateRequest},
        erasure_code::{Block, ErasureCode, ReedSolomon, Stripe},
        storage::{
            BlockId, BlockStorage, BufferEviction, FixedSizeSliceBuf, HDDStorage, PartialBlock,
            SliceBuffer, SliceOpt,
        },
    };

    use super::UpdateCtx;

    const BLOCK_NUM: usize = 36;
    const BLOCK_SIZE: usize = 1 << 20;
    const SLICE_SIZE: usize = 4 << 10;
    const SSD_BLOCK_CAP: usize = 12;
    const TEST_LOAD: usize = BLOCK_NUM * BLOCK_SIZE / SLICE_SIZE * 4;
    const EC_K: usize = 4;
    const EC_P: usize = 2;
    const EC_M: usize = EC_K + EC_P;
    #[ignore]
    #[test]
    fn test_do_update() {
        let ssd_dev = tempfile::tempdir().unwrap();
        let hdd_dev = tempfile::tempdir().unwrap();
        crate::data_builder::DataBuilder::new()
            .block_num(BLOCK_NUM)
            .block_size(BLOCK_SIZE)
            .hdd_dev_path(hdd_dev.path())
            .purge(true)
            .k_p(EC_K, EC_P)
            .build()
            .unwrap();
        let update_ctx = UpdateCtx {
            hdd_storage: HDDStorage::connect_to_dev(
                hdd_dev.path().to_path_buf(),
                NonZeroUsize::new(BLOCK_SIZE).unwrap(),
            )
            .unwrap(),
            block_size: BLOCK_SIZE,
            ec: ReedSolomon::from_k_p(
                NonZeroUsize::new(EC_K).unwrap(),
                NonZeroUsize::new(EC_P).unwrap(),
            ),
        };
        let mut block_ref = (0..BLOCK_NUM)
            .map(|block_id| {
                let block = update_ctx
                    .hdd_storage
                    .get_block_owned(block_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(block.len(), BLOCK_SIZE);
                block
            })
            .collect::<Vec<_>>();
        use rand::Rng;
        let updates = (0..TEST_LOAD)
            .map(|_| {
                let offset = rand::thread_rng().gen_range(0..BLOCK_SIZE / SLICE_SIZE);
                let offset = offset * SLICE_SIZE;
                let block_id = { (0..).map(|_| rand::thread_rng().gen_range(0..BLOCK_NUM)) }
                    .find(|id| (0..EC_K).contains(&(*id % EC_M)))
                    .unwrap();
                let slice_data = rand::thread_rng()
                    .sample_iter(rand::distributions::Standard)
                    .take(SLICE_SIZE)
                    .collect::<Vec<_>>();
                assert!(offset + slice_data.len() <= BLOCK_SIZE);
                UpdateRequest {
                    slice_data,
                    block_id,
                    offset,
                }
            })
            .collect::<Vec<_>>();
        let ssd_storage = FixedSizeSliceBuf::connect_to_dev(
            ssd_dev.path(),
            NonZeroUsize::new(BLOCK_SIZE).unwrap(),
            NonZeroUsize::new(SSD_BLOCK_CAP * BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let mut test_do_update = |block_id: BlockId, update_slices: Vec<SliceOpt>| {
            let block = block_ref.get_mut(block_id).unwrap();
            let mut off = 0;
            update_slices.iter().for_each(|update| {
                match update {
                    SliceOpt::Present(data) => {
                        block[off..off + data.len()].copy_from_slice(&data);
                        off += data.len();
                    }
                    SliceOpt::Absent(size) => {
                        off += size;
                    }
                };
            });
            assert_eq!(off, BLOCK_SIZE);
            do_update(&update_ctx, block_id, update_slices);
        };
        for UpdateRequest {
            slice_data,
            block_id,
            offset,
        } in updates
        {
            let evict = ssd_storage
                .push_slice(block_id, offset, slice_data.as_slice())
                .unwrap();
            if let Some(BufferEviction {
                block_id,
                data: PartialBlock { size, slices },
            }) = evict
            {
                debug_assert_eq!(size, BLOCK_SIZE);
                test_do_update(block_id, slices);
            };
        }
        while let Some(BufferEviction {
            block_id,
            data: PartialBlock { size, slices },
        }) = ssd_storage.pop()
        {
            debug_assert_eq!(size, BLOCK_SIZE);
            test_do_update(block_id, slices);
        }
        // check content
        block_ref
            .chunks_exact_mut(EC_M)
            .enumerate()
            .map(|(stripe_id, stripe)| {
                let mut stripe = Stripe::from_vec(
                    stripe
                        .iter()
                        .map(|block| Block::from(BytesMut::from(block.as_slice())))
                        .collect(),
                    NonZeroUsize::new(EC_K).unwrap(),
                    NonZeroUsize::new(EC_P).unwrap(),
                );
                update_ctx.ec.encode_stripe(&mut stripe).unwrap();
                let hdd_stripe = (0..EC_M)
                    .map(|idx| {
                        let block_id = idx + stripe_id * EC_M;
                        update_ctx
                            .hdd_storage
                            .get_block_owned(block_id)
                            .unwrap()
                            .unwrap()
                    })
                    .map(|block| Block::from(BytesMut::from(block.as_slice())))
                    .collect::<Vec<_>>();
                let hdd_stripe = Stripe::from_vec(
                    hdd_stripe,
                    NonZeroUsize::new(EC_K).unwrap(),
                    NonZeroUsize::new(EC_P).unwrap(),
                );
                (stripe, hdd_stripe)
            })
            .for_each(|(a, b)| assert_eq!(a, b));
    }
}
