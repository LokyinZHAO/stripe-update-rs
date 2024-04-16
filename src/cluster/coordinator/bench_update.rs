use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use range_collections::RangeSet;

use crate::{
    cluster::{
        messages::{coordinator_request::Request, worker_response::Response, TaskID},
        MessageQueueKey, Ranges, WorkerID,
    },
    erasure_code::{ErasureCode, PartialStripe, ReedSolomon},
    storage::{BlockId, EvictStrategySlice, MostModifiedStripeEvict},
    SUError, SUResult,
};

pub struct BenchUpdate {
    send_conn: redis::Connection,
    recv_conn: redis::Connection,
    request_queue_list: Vec<MessageQueueKey>,
    response_queue: MessageQueueKey,
    block_size: usize,
    slice_size: usize,
    buf_capacity: usize,
    block_num: usize,
    test_load: usize,
    k_p: (usize, usize),
}

impl TryFrom<CoordinatorBuilder> for BenchUpdate {
    type Error = SUError;

    fn try_from(value: CoordinatorBuilder) -> Result<Self, Self::Error> {
        let client = redis::Client::open(
            value
                .redis_url
                .ok_or_else(|| SUError::other("redis url not set"))?,
        )?;
        let send_conn = client.get_connection().map_err(|e| SUError::other(e))?;
        let recv_conn = client.get_connection().map_err(|e| SUError::other(e))?;
        let block_num = value
            .block_num
            .ok_or_else(|| SUError::Other("block number not set".into()))?;
        let k_p = value
            .k_p
            .ok_or_else(|| SUError::Other("k and p not set".into()))?;
        let worker_num = value
            .worker_num
            .ok_or_else(|| SUError::Other("worker number not set".into()))?;
        let request_queue_list = (1..=worker_num)
            .map(|i| i.try_into().unwrap())
            .map(WorkerID)
            .map(crate::cluster::format_request_queue_key)
            .collect();
        let response_queue = crate::cluster::format_response_queue_key();
        let block_size = value
            .block_size
            .ok_or_else(|| SUError::Other("block size not set".into()))?;
        let slice_size = value
            .slice_size
            .ok_or_else(|| SUError::Other("slice size not set".into()))?;
        let buf_capacity = value
            .buf_capacity
            .ok_or_else(|| SUError::Other("buffer capacity not set".into()))?;
        let test_load = value
            .test_load
            .ok_or_else(|| SUError::Other("test load not set".into()))?;
        Ok(Self {
            send_conn,
            recv_conn,
            request_queue_list,
            response_queue,
            block_size,
            slice_size,
            buf_capacity,
            block_num,
            test_load,
            k_p,
        })
    }
}

impl super::CoordinatorCmds for BenchUpdate {
    fn exec(self: Box<Self>) -> SUResult<()> {
        const CH_SIZE: usize = 32;
        let Self {
            mut send_conn,
            mut recv_conn,
            request_queue_list,
            response_queue,
            block_size,
            slice_size,
            mut block_num,
            k_p: (k, p),
            test_load,
            buf_capacity,
        } = *self;
        let worker_num = request_queue_list.len();
        let worker_id_range = 1..u8::try_from(worker_num).unwrap() + 1;
        let n = k + p;
        let stripe_num = block_num.div_ceil(n);
        if block_num % n != 0 {
            println!("ec-n [{n}] cannot divide block num [{block_num}], round up stripe number to {stripe_num}");
            block_num = stripe_num * n;
        }

        println!(
            "workers: {}",
            worker_id_range
                .clone()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!("stripe_num: {stripe_num}");
        println!("block num: {block_num}");
        println!("block size: {}", bytesize::ByteSize::b(block_size as u64));
        println!("slice size: {}", bytesize::ByteSize::b(slice_size as u64));
        println!("k: {k}");
        println!("p: {p}");

        // make sure redis is clean
        let _: () = redis::cmd("FLUSHALL")
            .query(&mut send_conn)
            .expect("fail to flush redis");

        // make sure workers are alive
        let alive_workers =
            super::broadcast_heartbeat(&request_queue_list, &response_queue, &mut recv_conn)?;
        if alive_workers
            != worker_id_range
                .clone()
                .map(|id| id.try_into().unwrap())
                .map(WorkerID)
                .collect::<Vec<_>>()
        {
            let offline_workers = worker_id_range
                .clone()
                .map(WorkerID)
                .filter(|id| !alive_workers.contains(id))
                .collect::<Vec<_>>();
            let offline_workers = offline_workers
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(SUError::Other(format!(
                "workers [{offline_workers}] are offline"
            )));
        }

        let (raw_request_producer, raw_request_consumer) =
            std::sync::mpsc::sync_channel::<Request>(CH_SIZE);
        let (request_producer, request_consumer) =
            std::sync::mpsc::sync_channel::<CoreItem>(CH_SIZE);
        let promise_map = Arc::new(Mutex::new(PromiseMap::new()));

        // generate requests
        let request_thread = std::thread::spawn(move || -> SUResult<()> {
            let param = UpdateRequestGeneratorParam {
                test_load,
                block_size,
                slice_size,
                block_num,
                k,
                p,
                send_ch: raw_request_producer,
            };
            update_request_generator(param)
        });

        // send requests
        let promise_map_send = Arc::clone(&promise_map);
        let send_thread = std::thread::spawn(move || -> SUResult<()> {
            let param = RequestSenderParam {
                request_consumer,
                send_conn,
                request_queue_list,
                promise_map: promise_map_send,
            };
            request_sender(param)
        });

        // core handler
        let core_handler_thread = std::thread::spawn(move || -> SUResult<()> {
            let param = CoreHandlerParam {
                raw_request_consumer,
                request_producer,
                buf_capacity,
                worker_id_range,
                k,
                p,
                block_size,
            };
            core_handler(param)
        });

        // receive ack
        let ack_thread = std::thread::spawn(move || -> SUResult<()> {
            let param = AckRecverParam {
                test_load,
                recv_conn,
                response_queue,
                promise_map,
            };
            ack_receiver(param)
        });

        ack_thread.join().unwrap()?;
        request_thread.join().unwrap()?;
        send_thread.join().unwrap()?;
        core_handler_thread.join().unwrap()?;
        Ok(())
    }
}

struct CoreItem {
    worker_id: WorkerID,
    request: Request,
    promise: Option<oneshot::Sender<Response>>,
}

type PromiseMap = std::collections::HashMap<TaskID, oneshot::Sender<Response>>;

struct UpdateRequestGeneratorParam {
    test_load: usize,
    block_size: usize,
    slice_size: usize,
    block_num: usize,
    k: usize,
    p: usize,
    send_ch: std::sync::mpsc::SyncSender<Request>,
}

fn update_request_generator(
    UpdateRequestGeneratorParam {
        test_load,
        slice_size,
        block_size,
        send_ch,
        block_num,
        k,
        p,
    }: UpdateRequestGeneratorParam,
) -> SUResult<()> {
    use rand::Rng;
    assert!(
        block_size % slice_size == 0,
        "block size must be multiple of slice size"
    );
    let slice_num = block_size / slice_size;
    let n = k + p;
    for _ in 0..test_load {
        let offset = rand::thread_rng().gen_range(0..slice_num);
        let offset = offset * slice_size;
        let slice_data = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(slice_size)
            .collect::<Bytes>();
        let range = offset..(offset + slice_size);
        let ranges = Ranges::new(range);
        let block_id = (0..)
            .map(|_| rand::thread_rng().gen_range(0..block_num))
            .find(|id| (0..k).contains(&(*id % n)))
            .unwrap();
        debug_assert!(offset + slice_data.len() <= block_size);
        let request = Request::buffer_update_data(block_id, ranges, slice_data);
        send_ch.send(request).expect("channel disconnected");
    }
    Ok(())
}

struct RequestSenderParam {
    request_consumer: std::sync::mpsc::Receiver<CoreItem>,
    send_conn: redis::Connection,
    request_queue_list: Vec<MessageQueueKey>,
    promise_map: Arc<Mutex<PromiseMap>>,
}

fn request_sender(
    RequestSenderParam {
        request_consumer,
        mut send_conn,
        request_queue_list,
        promise_map,
    }: RequestSenderParam,
) -> SUResult<()> {
    let worker_id_to_queue_key =
        |worker_id: WorkerID| -> &MessageQueueKey { &request_queue_list[worker_id.0 as usize - 1] };
    while let Ok(CoreItem {
        worker_id,
        request,
        promise,
    }) = request_consumer.recv()
    {
        let key = worker_id_to_queue_key(worker_id);
        if let Some(promise) = promise {
            let task_id = request.id;
            let old_promise = promise_map
                .lock()
                .expect("fail to unlock")
                .insert(task_id, promise);
            assert!(old_promise.is_none(), "promise already exists");
        }
        request.push_to_redis(&mut send_conn, key)?;
    }
    Ok(())
}

struct AckRecverParam {
    test_load: usize,
    recv_conn: redis::Connection,
    response_queue: MessageQueueKey,
    promise_map: Arc<Mutex<PromiseMap>>,
}

fn ack_receiver(
    AckRecverParam {
        mut recv_conn,
        response_queue,
        test_load,
        promise_map,
    }: AckRecverParam,
) -> SUResult<()> {
    use crate::cluster::messages::worker_response::Ack as ResponseAck;
    use crate::cluster::messages::worker_response::Nak as ResponseNak;
    let progress_bar = indicatif::ProgressBar::new(test_load as u64);
    let mut cur_load = 0;
    while cur_load != test_load {
        let response = Response::fetch_from_redis(&mut recv_conn, &response_queue)?;
        match &response.head {
            Ok(ResponseAck::BufferUpdateData) => {
                cur_load += 1;
                progress_bar.inc(1);
            }
            Ok(ResponseAck::Update) => (/* nothing to do */),
            Ok(ResponseAck::PersistUpdate { .. }) | Ok(ResponseAck::RetrieveData { .. }) => {
                let task_id = response.id;
                let mut promise_map_lock = promise_map.lock().expect("fail to unlock");
                let promise = promise_map_lock
                    .remove(&task_id)
                    .expect("promise not found");
                drop(promise_map_lock);
                promise.send(response).expect("send response failed");
            }
            Err(ResponseNak { .. }) => {
                panic!(
                    "nak response: {}",
                    String::from_utf8(response.payload.unwrap().into()).unwrap()
                )
            }
            Ok(ResponseAck::DropStore { .. })
            | Ok(ResponseAck::FlushBuf { .. })
            | Ok(ResponseAck::HeartBeat { .. })
            | Ok(ResponseAck::Shutdown { .. })
            | Ok(ResponseAck::StoreBlock) => {
                unreachable!("bad response")
            }
        };
    }
    println!("done!");
    Ok(())
}

use crate::cluster::messages::coordinator_request::Head as RequestHead;

use super::CoordinatorBuilder;
struct CoreHandlerParam {
    raw_request_consumer: std::sync::mpsc::Receiver<Request>,
    request_producer: std::sync::mpsc::SyncSender<CoreItem>,
    buf_capacity: usize,
    worker_id_range: std::ops::Range<u8>,
    k: usize,
    p: usize,
    block_size: usize,
}

fn core_handler(
    CoreHandlerParam {
        raw_request_consumer,
        request_producer,
        worker_id_range,
        buf_capacity,
        k,
        p,
        block_size,
    }: CoreHandlerParam,
) -> SUResult<()> {
    #[inline]
    fn extract_block_id(request: &Request) -> BlockId {
        if let RequestHead::BufferUpdateData { id, .. } = &request.head {
            *id
        } else {
            unreachable!("bad request")
        }
    }
    let worker_num = worker_id_range.end - worker_id_range.start;
    let block_id_to_worker_id = move |block_id: usize| -> WorkerID {
        let worker_id = block_id % (worker_num as usize);
        WorkerID(worker_id as u8 + worker_id_range.start)
    };
    let n = k + p;
    let mut check_buffer_param = CheckBufferThresholdParam {
        request_producer: request_producer.clone(),
        evict: MostModifiedStripeEvict::new(
            NonZeroUsize::new(n).unwrap(),
            NonZeroUsize::new(buf_capacity).unwrap(),
        ),
        erasure_code: ReedSolomon::from_k_p(
            NonZeroUsize::new(k).unwrap(),
            NonZeroUsize::new(p).unwrap(),
        ),
        block_id_to_worker_id: Box::new(block_id_to_worker_id),
        block_size,
    };
    while let Ok(request) = raw_request_consumer.recv() {
        let block_id = extract_block_id(&request);
        let worker_id = block_id_to_worker_id(block_id);
        let item = CoreItem {
            worker_id,
            request: request.clone(),
            promise: None, /* update request does not promise a response */
        };
        request_producer.send(item).expect("channel disconnected");
        check_buffer_threshold(&mut check_buffer_param, &request)?;
    }
    Ok(())
}

struct CheckBufferThresholdParam<EV: EvictStrategySlice, EC: ErasureCode> {
    request_producer: std::sync::mpsc::SyncSender<CoreItem>,
    block_size: usize,
    evict: EV,
    erasure_code: EC,
    block_id_to_worker_id: Box<dyn Fn(BlockId) -> WorkerID>,
}

fn check_buffer_threshold<EV: EvictStrategySlice, EC: ErasureCode>(
    param: &mut CheckBufferThresholdParam<EV, EC>,
    request: &Request,
) -> SUResult<()> {
    let (id, range) = match &request.head {
        RequestHead::BufferUpdateData { id, ranges, .. } => {
            let ranges = ranges.to_ranges();
            assert_eq!(ranges.len(), 1, "bad ranges");
            eprintln!("[DEBUG] update buf: {}", id);
            (*id, ranges.into_iter().next().unwrap())
        }
        _ => unreachable!("bad request"),
    };
    let eviction = param.evict.push(id, range);
    if let Some(eviction) = eviction {
        handle_eviction::<EV, EC>(param, eviction)?;
    }
    Ok(())
}

fn handle_eviction<EV: EvictStrategySlice, EC: ErasureCode>(
    CheckBufferThresholdParam {
        request_producer,
        evict,
        erasure_code,
        block_id_to_worker_id,
        block_size,
    }: &mut CheckBufferThresholdParam<EV, EC>,
    (block_id, range): (BlockId, crate::storage::RangeSet),
) -> SUResult<()> {
    use crate::cluster::messages::worker_response::Ack as ResponseAck;
    // eviction occurs
    let k = erasure_code.k() as usize;
    let p = erasure_code.p() as usize;
    let n = k + p;

    let stripe_id = block_id / n;
    let block_idx = block_id % n;
    assert!((0..k).contains(&block_idx));
    let source_block_id_range = stripe_id * n..stripe_id * n + k;
    let stripe_ranges = source_block_id_range
        .clone()
        .take(block_idx)
        .map(|block_id| evict.pop_with_id(block_id))
        .chain(std::iter::once(Some(range.clone())))
        .chain(
            source_block_id_range
                .clone()
                .skip(block_idx + 1)
                .map(|block_id| evict.pop_with_id(block_id)),
        )
        .collect::<Vec<_>>();
    eprintln!(
        "[DEBUG] evict block buf: {}",
        stripe_ranges
            .iter()
            .zip(source_block_id_range.clone())
            .filter(|(r, _)| r.is_some())
            .map(|(r, id)| format!("[block id {id}, range {:?}]", r))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let range_union: Ranges = stripe_ranges
        .iter()
        .filter_map(|r| r.as_ref())
        .fold(RangeSet::empty(), |acc: RangeSet<[usize; 2]>, element| {
            acc.union(element)
        })
        .into();
    // retrieve corresponding parity data
    let parity_id_range = stripe_id * n + k..stripe_id * n + n;
    let parity_data_future = parity_id_range
        .clone()
        .map(|block_id| {
            let request = Request::retrieve_slice(block_id, range_union.clone());
            let (promise, future) = oneshot::channel();
            let core_item = CoreItem {
                worker_id: block_id_to_worker_id(block_id),
                request,
                promise: Some(promise),
            };
            request_producer
                .send(core_item)
                .expect("channel disconnected");
            future
        })
        .collect::<Vec<_>>();
    // persist source data update, and retrieve the update delta
    let source_data_future = stripe_ranges
        .iter()
        .zip(source_block_id_range.clone())
        .map(|(range, block_id)| {
            if range.is_some() {
                let request = Request::persist_update(block_id);
                let (promise, future) = oneshot::channel();
                let core_item = CoreItem {
                    worker_id: block_id_to_worker_id(block_id),
                    request,
                    promise: Some(promise),
                };
                request_producer
                    .send(core_item)
                    .expect("channel disconnected");
                Some(future)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let mut partial_stripe = PartialStripe::make_absent_from_k_p(
        NonZeroUsize::new(k).unwrap(),
        NonZeroUsize::new(p).unwrap(),
        NonZeroUsize::new(*block_size).unwrap(),
    );
    let (source, parity) = partial_stripe.split_mut_source_parity();
    // fill source data with zero
    source
        .iter_mut()
        .zip_eq(source_data_future.iter())
        .filter(|(_, future)| future.is_some())
        .for_each(|(block, _)| *block = Some(BytesMut::zeroed(*block_size).into()));
    // get parity data
    parity_data_future.into_iter().zip_eq(parity).try_for_each(
        |(future, block_opt)| -> SUResult<()> {
            let response = future.recv().expect("future dropped");
            let mut payload = if let Ok(ResponseAck::RetrieveData { .. }) = response.head {
                response.payload.unwrap()
            } else {
                unreachable!("bad response");
            };
            let mut block_data = BytesMut::zeroed(*block_size);
            let ranges = range_union.to_ranges();
            ranges
                .iter()
                .map(|range| range.len())
                .map(|len| payload.split_to(len))
                .zip(ranges.iter().cloned())
                .for_each(|(slice_data, range)| block_data[range].copy_from_slice(&slice_data));
            *block_opt = Some(block_data.into());
            Ok(())
        },
    )?;
    // get source data, and perform delta update
    source_data_future
        .into_iter()
        .enumerate()
        .filter(|(_, future_opt)| future_opt.is_some())
        .try_for_each(|(inner_stripe_idx, future_opt)| -> SUResult<()> {
            let future = future_opt.unwrap();
            let response = future.recv().expect("future dropped");
            let (ranges, mut payload) =
                if let Ok(ResponseAck::PersistUpdate { ranges, .. }) = response.head {
                    (ranges.to_ranges(), response.payload.unwrap())
                } else {
                    unreachable!("bad response");
                };
            ranges
                .iter()
                .map(|range| range.len())
                .map(|len| payload.split_to(len))
                .zip(ranges.iter())
                .try_for_each(|(delta_data, range)| {
                    // encode in delta update manner
                    erasure_code.delta_update(
                        &delta_data,
                        inner_stripe_idx,
                        range.start,
                        &mut partial_stripe,
                    )
                })?;
            Ok(())
        })?;
    // update parity request
    partial_stripe
        .split_mut_source_parity()
        .1 // get parity
        .iter()
        .zip_eq(parity_id_range)
        .for_each(|(block, block_id)| {
            let block = block.as_ref().take().expect("missing parity data");
            let mut payload = BytesMut::with_capacity(range_union.len());
            range_union
                .to_ranges()
                .iter()
                .cloned()
                .for_each(|range| payload.extend_from_slice(&block[range]));
            let request = Request::update(block_id, range_union.clone(), payload.freeze());
            let core_item = CoreItem {
                worker_id: block_id_to_worker_id(block_id),
                request,
                promise: None,
            };
            request_producer
                .send(core_item)
                .expect("channel disconnected");
        });

    Ok(())
}
