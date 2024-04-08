use std::num::NonZeroUsize;

use indicatif::ProgressIterator;
use itertools::zip_eq;

use crate::{
    cluster::{
        messages::{CoordinatorRequest, WorkerResponse},
        progress_style_template, WorkerID,
    },
    erasure_code::{ErasureCode, ReedSolomon, Stripe},
    SUError, SUResult,
};

use super::Coordinator;

impl Coordinator {
    pub fn build_data(self) -> SUResult<()> {
        const CH_SIZE: usize = 32;
        let request_list = self.config.request_queue_list.clone();
        let worker_id_range = 1..request_list.len() + 1;
        let response_list = self.config.response_queue.clone();
        let block_size = self.config.block_size;
        let mut block_num = self.config.block_num;
        let (k, p) = self.config.k_p;
        let n = k + p;
        let stripe_num = block_num.div_ceil(n);
        if block_num % n != 0 {
            println!("ec-n [{n}] cannot divide block num [{block_num}], round up stripe number to {stripe_num}");
            block_num = stripe_num * n;
        }
        // print configuration
        println!(
            "block size: {block_size}
            block num: {block_num}
            worker num: {}
            k: {k}
            p: {p}
            stripe num: {stripe_num}",
            worker_id_range.len()
        );

        // connect to redis
        let mut conn = self
            .client
            .get_connection()
            .expect("fail to get redis connection");

        // make sure redis is clean
        let _: () = redis::cmd("FLUSHALL")
            .query(&mut conn)
            .expect("fail to flush redis");

        // make sure workers are alive
        let alive_workers = self.broadcast_heartbeat(&mut conn)?;
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

        type StripeItem = Vec<CoordinatorRequest>;
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
                    .map(|(payload, id)| CoordinatorRequest::StoreBlock {
                        id,
                        payload: payload.to_vec(),
                    })
                    .collect::<Vec<_>>();
                stripe_producer.send(item).unwrap();
            });
        });

        let dispatcher_handle = std::thread::spawn(move || {
            while let Ok(item) = stripe_consumer.recv() {
                std::iter::zip(item, request_list.iter().cycle())
                    .for_each(|(request, key)| request.try_push_to_redis(&mut conn, key).unwrap());
            }
        });

        let mut conn = self.client.get_connection().unwrap();
        let ack_handle = std::thread::spawn(move || {
            (0..block_num)
                .progress_with_style(progress_style_template(Some("block stored")))
                .for_each(|_| {
                    let response = WorkerResponse::try_fetch_from_redis(&mut conn, &response_list)
                        .expect("redis error");
                    match response {
                        WorkerResponse::StoreBlock => (),
                        WorkerResponse::Nak(err) => panic!("nak of storing blocks: {err}"),
                        _ => unreachable!("unexpected response"),
                    }
                });
        });

        stripe_maker_handle.join().unwrap();
        dispatcher_handle.join().unwrap();
        ack_handle.join().unwrap();

        Ok(())
    }
}
