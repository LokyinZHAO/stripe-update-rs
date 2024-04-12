use crate::{
    cluster::{
        messages::CoordinatorRequestHead, progress_style_template, MessageQueueKey, WorkerID,
    },
    SUError, SUResult,
};

struct BenchUpdate {
    send_conn: redis::Connection,
    recv_conn: redis::Connection,
    request_queue_list: Vec<MessageQueueKey>,
    response_queue: MessageQueueKey,
    block_size: usize,
    slice_size: usize,
    block_num: usize,
    test_load: usize,
    k_p: (usize, usize),
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
        } = *self;
        let worker_num = request_queue_list.len();
        let worker_id_range = 1..worker_num + 1;
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

        type Item = (WorkerID, CoordinatorRequestHead);
        let (_request_producer, request_consumer) = std::sync::mpsc::sync_channel::<Item>(CH_SIZE);
        let (ack_notifier, ack_watcher) = std::sync::mpsc::sync_channel(CH_SIZE);

        // generate requests
        let request_generator = move || -> SUResult<()> {
            // (0..test_load).try_for_each(|_| );
            Ok(())
        };

        // send requests
        let request_sender = move || -> SUResult<()> {
            while let Ok((id, request)) = request_consumer.recv() {
                let key = &request_queue_list[id.0 - 1];
                request.try_push_to_redis(&mut send_conn, key)?;
                ack_notifier
                    .send(())
                    .map_err(|_| SUError::Other("ack watcher disconnected".into()))?;
            }
            Ok(())
        };

        // receive ack
        let ack_receiver = move || -> SUResult<()> {
            use indicatif::ProgressIterator;
            (0..test_load)
                .progress_with_style(progress_style_template(Some("benchmarking")))
                .try_for_each(|_| {
                    ack_watcher
                        .recv()
                        .map_err(|_| SUError::Other("ack notifier disconnected".into()))
                })?;
            Ok(())
        };

        let request_thread = std::thread::spawn(request_generator);
        let send_thread = std::thread::spawn(request_sender);
        let ack_thread = std::thread::spawn(ack_receiver);

        request_thread.join().unwrap()?;
        send_thread.join().unwrap()?;
        ack_thread.join().unwrap()?;
        Ok(())
    }
}
