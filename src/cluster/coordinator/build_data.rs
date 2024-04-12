use std::num::NonZeroUsize;

use indicatif::ProgressIterator;
use itertools::zip_eq;

use crate::{
    cluster::{
        messages::{
            coordinator_request::Request,
            worker_response::{Ack, Response},
        },
        progress_style_template, MessageQueueKey, WorkerID,
    },
    erasure_code::{ErasureCode, ReedSolomon, Stripe},
    SUError, SUResult,
};

pub struct BuildData {
    recv_conn: redis::Connection,
    send_conn: redis::Connection,
    request_queue_list: Vec<MessageQueueKey>,
    response_queue: MessageQueueKey,
    block_size: usize,
    block_num: usize,
    k_p: (usize, usize),
}

impl TryFrom<super::CoordinatorBuilder> for BuildData {
    type Error = SUError;

    fn try_from(value: super::CoordinatorBuilder) -> Result<Self, Self::Error> {
        let redis_url = value
            .redis_url
            .ok_or_else(|| SUError::Other("redis url not set".into()))?;
        let worker_num = value
            .worker_num
            .ok_or_else(|| SUError::Other("worker number not set".into()))?;
        let block_size = value
            .block_size
            .ok_or_else(|| SUError::Other("block size not set".into()))?;
        let block_num = value
            .block_num
            .ok_or_else(|| SUError::Other("block number not set".into()))?;
        let k_p = value
            .k_p
            .ok_or_else(|| SUError::Other("k and p not set".into()))?;
        let client = redis::Client::open(redis_url)?;
        let request_queue_list = (1..=worker_num)
            .map(|i| i.try_into().unwrap())
            .map(WorkerID)
            .map(crate::cluster::format_request_queue_key)
            .collect();
        let response_queue = crate::cluster::format_response_queue_key();
        Ok(Self {
            recv_conn: client.get_connection()?,
            send_conn: client.get_connection()?,
            request_queue_list,
            response_queue,
            block_size,
            block_num,
            k_p,
        })
    }
}

impl super::CoordinatorCmds for BuildData {
    fn exec(self: Box<Self>) -> SUResult<()> {
        const CH_SIZE: usize = 32;
        let request_queue_list = self.request_queue_list;
        let response_queue = self.response_queue.clone();
        let worker_id_range = 1_u8..u8::try_from(request_queue_list.len()).unwrap() + 1;
        let block_size = self.block_size;
        let mut recv_conn = self.recv_conn;
        let mut send_conn = self.send_conn;
        let mut block_num = self.block_num;
        let (k, p) = self.k_p;
        let n = k + p;
        let stripe_num = block_num.div_ceil(n);
        if block_num % n != 0 {
            println!("ec-n [{n}] cannot divide block num [{block_num}], round up stripe number to {stripe_num}");
            block_num = stripe_num * n;
        }
        // print configuration
        println!(
            "block size: {}
            block num: {block_num}
            worker num: {}
            k: {k}
            p: {p}
            stripe num: {stripe_num}",
            bytesize::ByteSize::b(block_size as u64),
            worker_id_range.len()
        );

        // make sure redis is clean
        let _: () = redis::cmd("FLUSHALL")
            .query(&mut send_conn)
            .expect("fail to flush redis");

        // make sure workers are alive
        let alive_workers =
            super::broadcast_heartbeat(&request_queue_list, &response_queue, &mut recv_conn)?;
        if alive_workers != worker_id_range.clone().map(WorkerID).collect::<Vec<_>>() {
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

        type StripeItem = Vec<Request>;
        let (stripe_producer, stripe_consumer) =
            std::sync::mpsc::sync_channel::<StripeItem>(CH_SIZE);

        let stripe_maker_handle = std::thread::spawn(move || {
            use rand::Rng;
            let rs =
                ReedSolomon::from_k_p(NonZeroUsize::new(k).unwrap(), NonZeroUsize::new(p).unwrap());
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
                rs.encode_stripe(&mut stripe)
                    .expect("fail to encode stripe");
                let block_id_range = (stripe_id * n)..(stripe_id * n + n);
                let item = zip_eq(stripe.into_blocks(), block_id_range)
                    .map(|(payload, id)| Request::store_block(id, payload.into()))
                    .collect::<Vec<_>>();
                stripe_producer.send(item).unwrap();
            });
        });

        let dispatcher_handle = std::thread::spawn(move || {
            while let Ok(item) = stripe_consumer.recv() {
                std::iter::zip(item, request_queue_list.iter().cycle())
                    .try_for_each(|(request, key)| request.push_to_redis(&mut send_conn, key))
                    .expect("fail to dispatch stripe");
            }
        });

        let ack_handle = std::thread::spawn(move || {
            (0..block_num)
                .progress_with_style(progress_style_template(Some("block stored")))
                .try_for_each(|_| {
                    let response = Response::fetch_from_redis(&mut recv_conn, &response_queue)?;
                    match &response.head {
                        Ok(Ack::StoreBlock) => Ok(()),
                        Err(_) => Err(SUError::other(format!(
                            "nak: {}",
                            String::from_utf8(response.payload.unwrap()).unwrap()
                        ))),
                        _ => unreachable!("unexpected response"),
                    }
                })
                .expect("fail to wait for acks");
        });

        stripe_maker_handle.join().unwrap();
        dispatcher_handle.join().unwrap();
        ack_handle.join().unwrap();

        Ok(())
    }
}
