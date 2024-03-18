use std::{
    io::Write,
    num::NonZeroUsize,
    ops::Range,
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::BytesMut;
use indicatif::ProgressIterator;
use range_collections::{RangeSet, RangeSet2};

use crate::{
    erasure_code::{Block, ErasureCode, PartialStripe, ReedSolomon, Stripe},
    standalone_cmds::bench::UpdateRequest,
    standalone_cmds::dev_display,
    storage::{
        BlockId, BufferEviction, EvictStrategySlice, FixedSizeSliceBuf, HDDStorage,
        MostModifiedStripeEvict, PartialBlock, SliceBuffer, SliceOpt, SliceStorage, StripeId,
    },
    SUResult,
};

pub fn rangeset_to_ranges(range_set: RangeSet2<usize>) -> Vec<Range<usize>> {
    range_set
        .boundaries()
        .chunks_exact(2)
        .map(|bound| bound[0]..bound[1])
        .collect()
}

use super::Bench;
#[derive(Debug)]
struct UpdateCtx<EC: ErasureCode, EV: EvictStrategySlice> {
    hdd_storage: HDDStorage,
    block_size: usize,
    slice_buf: FixedSizeSliceBuf<EV>,
    ec: EC,
}

fn fetch_stripe<EC: ErasureCode, EV: EvictStrategySlice>(
    UpdateCtx {
        hdd_storage: _,
        block_size: _,
        slice_buf,
        ec,
    }: &UpdateCtx<EC, EV>,
    block_id: BlockId,
    update_slice: Vec<SliceOpt>,
) -> (StripeId, Vec<Option<Vec<SliceOpt>>>) {
    let m = ec.m();
    let k = ec.k();
    let stripe_id = StripeId::from(block_id / ec.m());
    let source_block_id_range = stripe_id.into_inner() * m..stripe_id.into_inner() * m + k;
    let mut updates = source_block_id_range
        .map(|block_id| slice_buf.pop_one(block_id).map(|e| e.data.slices))
        .collect::<Vec<_>>();
    updates[block_id % m] = Some(update_slice);
    (stripe_id, updates)
}

fn do_update<EC: ErasureCode, EV: EvictStrategySlice>(
    UpdateCtx {
        hdd_storage,
        block_size,
        ec,
        slice_buf: _,
    }: &UpdateCtx<EC, EV>,
    stripe_id: StripeId,
    stripe_update_slices: Vec<Option<Vec<SliceOpt>>>,
) {
    let k = ec.k();
    let block_size = *block_size;
    let p = ec.p();
    let m = ec.m();
    let source_block_id_range = stripe_id.into_inner() * m..stripe_id.into_inner() * m + k;
    debug_assert_eq!(stripe_update_slices.len(), k);
    let update_src_block_num = stripe_update_slices
        .iter()
        .filter(|opt| opt.is_some())
        .count();
    let union_range = stripe_update_slices
        .iter()
        .filter(|update_slice| update_slice.is_some())
        .map(|update_slice| {
            let mut range_set: RangeSet2<usize> = RangeSet::empty();
            let mut offset = 0;
            update_slice
                .as_ref()
                .unwrap()
                .iter()
                .for_each(|update| match update {
                    SliceOpt::Present(slice) => {
                        range_set.union_with(&RangeSet2::from(offset..offset + slice.len()));
                        offset += slice.len();
                    }
                    SliceOpt::Absent(size) => offset += size,
                });
            range_set
        })
        .fold(RangeSet2::<usize>::empty(), |acc, this| acc.union(&this));
    let union_range = rangeset_to_ranges(union_range);
    let is_full_update = update_src_block_num == k;
    let mut buf = BytesMut::zeroed(block_size * (update_src_block_num + p));
    let mut partial_stripe = PartialStripe::make_absent_from_k_p(
        NonZeroUsize::new(k).unwrap(),
        NonZeroUsize::new(p).unwrap(),
        NonZeroUsize::new(block_size).unwrap(),
    );
    stripe_update_slices
        .iter()
        .zip(source_block_id_range)
        .filter(|(source_update, _)| source_update.is_some())
        .for_each(|(_, block_id)| {
            let mut source_data = buf.split_to(block_size);
            union_range.iter().for_each(|range| {
                hdd_storage
                    .get_slice(block_id, range.start, &mut source_data[range.to_owned()])
                    .unwrap()
                    .unwrap();
            });
            let ret = partial_stripe.replace_block(block_id % m, Some(Block::from(source_data)));
            debug_assert!(ret.is_none());
        });
    (stripe_id.into_inner() * m + k..stripe_id.into_inner() * m + m).for_each(|block_id| {
        let mut parity_data = buf.split_to(block_size);
        union_range.iter().for_each(|range| {
            hdd_storage
                .get_slice(block_id, range.start, &mut parity_data[range.to_owned()])
                .unwrap()
                .unwrap();
        });
        let ret = partial_stripe.replace_block(block_id % m, Some(Block::from(parity_data)));
        debug_assert!(ret.is_none());
    });

    if is_full_update {
        let mut stripe = Stripe::try_from(partial_stripe).unwrap();
        ec.encode_stripe(&mut stripe).unwrap();
        stripe
            .iter_source()
            .chain(stripe.iter_parity())
            .zip(stripe_id.into_inner() * m..stripe_id.into_inner() * m + m)
            .for_each(|(block, block_id)| {
                union_range.iter().for_each(|range| {
                    hdd_storage
                        .put_slice(block_id, range.start, &block[range.to_owned()])
                        .unwrap()
                        .unwrap()
                })
            });
    } else {
        partial_stripe.iter_present().for_each(|(idx, block_data)| {
            let block_id = stripe_id.into_inner() * m + idx;
            union_range.iter().for_each(|range| {
                hdd_storage
                    .put_slice(block_id, range.start, &block_data[range.to_owned()])
                    .unwrap()
                    .unwrap()
            })
        });
    }
}

impl Bench {
    pub(super) fn merge_stripe(&self) -> SUResult<()> {
        const CHANNEL_SIZE: usize = 64;
        struct Ack();
        let sync_channel = std::sync::mpsc::sync_channel::<UpdateRequest>(CHANNEL_SIZE);
        let (update_producer, update_consumer) = sync_channel;
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
        let buffer_len_monitor = Arc::new(AtomicUsize::new(0));
        let buffer_len_updater = Arc::clone(&buffer_len_monitor);
        let encoder_handle = std::thread::spawn(move || {
            let ec =
                ReedSolomon::from_k_p(NonZeroUsize::new(k).unwrap(), NonZeroUsize::new(p).unwrap());
            let hdd_storage =
                HDDStorage::connect_to_dev(hdd_dev_path, NonZeroUsize::new(block_size).unwrap())
                    .unwrap();
            let ssd_storage = FixedSizeSliceBuf::connect_to_dev_with_evict(
                ssd_dev_path,
                NonZeroUsize::new(block_size).unwrap(),
                MostModifiedStripeEvict::new(
                    NonZeroUsize::new(m).unwrap(),
                    NonZeroUsize::new(ssd_cap).unwrap(),
                ),
            )
            .unwrap();
            let mut duration = std::time::Duration::ZERO;
            let mut cnt = 0_usize;
            let update_ctx = UpdateCtx::<ReedSolomon, MostModifiedStripeEvict> {
                hdd_storage,
                block_size,
                slice_buf: ssd_storage,
                ec,
            };
            while let Ok(UpdateRequest {
                slice_data,
                block_id,
                offset,
            }) = update_consumer.recv()
            {
                let epoch = std::time::Instant::now();
                let evict = update_ctx
                    .slice_buf
                    .push_slice(block_id, offset, slice_data.as_slice())
                    .unwrap();
                if let Some(BufferEviction {
                    block_id,
                    data: PartialBlock { size, slices },
                }) = evict
                {
                    debug_assert_eq!(size, block_size);
                    let (stripe_id, updates) = fetch_stripe(&update_ctx, block_id, slices);
                    do_update(&update_ctx, stripe_id, updates);
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
            }) = update_ctx.slice_buf.pop()
            {
                let epoch = std::time::Instant::now();
                debug_assert_eq!(size, block_size);
                let (stripe_id, updates) = fetch_stripe(&update_ctx, block_id, slices);
                do_update(&update_ctx, stripe_id, updates);
                duration += epoch.elapsed();
                cnt += 1;
                ack_producer.send(Ack()).unwrap();
                buffer_len_updater.store(
                    ssd_cap - update_ctx.slice_buf.len(),
                    std::sync::atomic::Ordering::SeqCst,
                );
            }
            (duration, cnt)
        });

        // ack: show progress
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
}

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use bytes::BytesMut;

    use crate::{
        erasure_code::{Block, ErasureCode, ReedSolomon, Stripe},
        standalone_cmds::bench::{
            merge_stripe::{do_update, fetch_stripe},
            UpdateRequest,
        },
        storage::{
            BlockId, BlockStorage, BufferEviction, FixedSizeSliceBuf, HDDStorage,
            MostModifiedStripeEvict, PartialBlock, SliceBuffer, SliceOpt,
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
        crate::standalone_cmds::data_builder::DataBuilder::new()
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
            slice_buf: FixedSizeSliceBuf::connect_to_dev_with_evict(
                ssd_dev.path().to_path_buf(),
                NonZeroUsize::new(BLOCK_SIZE).unwrap(),
                MostModifiedStripeEvict::new(
                    NonZeroUsize::new(EC_M).unwrap(),
                    NonZeroUsize::new(SSD_BLOCK_CAP * BLOCK_SIZE).unwrap(),
                ),
            )
            .unwrap(),
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
            let (stripe_id, updates) = fetch_stripe(&update_ctx, block_id, update_slices);
            do_update(&update_ctx, stripe_id, updates);
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
