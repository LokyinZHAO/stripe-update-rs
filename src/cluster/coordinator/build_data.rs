use std::{io::Write, num::NonZeroUsize};

use indicatif::ProgressIterator;
use itertools::zip_eq;
use redis::Commands;

use crate::{
    cluster::{
        messages::{CoordinatorRequest, WorkerResponse},
        progress_style_template,
    },
    erasure_code::{ErasureCode, ReedSolomon, Stripe},
    SUResult,
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
        let purge = self.config.purge;
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

        // clean up
        if purge {
            print!("purging dir...");
            std::io::stdout().flush().unwrap();
            request_list.iter().for_each(|key| {
                let _: () = conn
                    .rpush(key, CoordinatorRequest::FlushBuf)
                    .expect("redis send error");
                let _: () = conn
                    .rpush(key, CoordinatorRequest::DropStore)
                    .expect("redis send error");
            });
            worker_id_range.for_each(|_| {
                let response: WorkerResponse = conn
                    .blpop(response_list.as_str(), 0_f64)
                    .expect("redis error");
                match response {
                    WorkerResponse::FlushBuf => (),
                    WorkerResponse::DropStore => (),
                    WorkerResponse::Nak(err) => panic!("nak of purging: {err}"),
                    _ => unreachable!("unexpected response"),
                }
            });
            println!("done");
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
                let block_id_range = (stripe_id * p)..(stripe_id * p + p);
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
                itertools::zip_eq(item, request_list.iter())
                    .for_each(|(request, key)| conn.rpush(key, request).unwrap());
            }
        });

        let mut conn = self.client.get_connection().unwrap();
        let ack_handle = std::thread::spawn(move || {
            (0..block_num)
                .progress_with_style(progress_style_template(Some("block stored")))
                .for_each(|_| {
                    let response: WorkerResponse = conn
                        .rpop(response_list.as_str(), None)
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
