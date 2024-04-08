use std::io::Write;

use redis::Commands;

use crate::{
    cluster::{
        format_request_queue_key,
        messages::{CoordinatorRequest, WorkerResponse},
        MessageQueueKey, WorkerID,
    },
    SUError, SUResult,
};

use super::CoordinatorCmds;

pub struct Purge {
    conn: redis::Connection,
    request_queue_list: Vec<MessageQueueKey>,
    response_queue: MessageQueueKey,
}

impl TryFrom<super::CoordinatorBuilder> for Purge {
    type Error = SUError;

    fn try_from(value: super::CoordinatorBuilder) -> Result<Self, Self::Error> {
        let redis_url = value
            .redis_url
            .ok_or_else(|| SUError::Other("redis url not set".into()))?;
        let worker_num = value
            .worker_num
            .ok_or_else(|| SUError::Other("worker number not set".into()))?;
        Ok(Purge {
            conn: redis::Client::open(redis_url)?.get_connection()?,
            request_queue_list: (1..=worker_num)
                .map(WorkerID)
                .map(format_request_queue_key)
                .collect(),
            response_queue: super::format_response_queue_key(),
        })
    }
}
impl CoordinatorCmds for Purge {
    fn exec(mut self: Box<Self>) -> SUResult<()> {
        let worker_num = self.request_queue_list.len();
        let worker_id_range = 1..worker_num + 1;

        redis::cmd("FLUSHALL").query(&mut self.conn)?;
        // get alive workers
        let alive_workers = super::broadcast_heartbeat(
            &self.request_queue_list,
            &self.response_queue,
            &mut self.conn,
        )?;
        println!(
            "alive workers: {}",
            alive_workers
                .iter()
                .map(WorkerID::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!("purging dir...");
        self.request_queue_list.iter().for_each(|key| {
            let _: () = self
                .conn
                .rpush(key, CoordinatorRequest::FlushBuf)
                .expect("redis send error");
            let _: () = self
                .conn
                .rpush(key, CoordinatorRequest::DropStore)
                .expect("redis send error");
        });
        let mut flush_buf_ack = vec![false; worker_num];
        let mut drop_ack = vec![false; worker_num];
        (0..worker_num * 2).try_for_each(|_| -> SUResult<()> {
            let flush_response =
                WorkerResponse::try_fetch_from_redis(&mut self.conn, &self.response_queue)?;
            match flush_response {
                WorkerResponse::FlushBuf(WorkerID(worker_id)) => {
                    flush_buf_ack[worker_id - 1] = true;
                    Ok(())
                }
                WorkerResponse::DropStore(WorkerID(worker_id)) => {
                    drop_ack[worker_id - 1] = true;
                    Ok(())
                }
                WorkerResponse::Nak(err) => Err(SUError::Other(format!("worker error: {err}"))),
                _ => unreachable!("unexpected response"),
            }
        })?;
        use itertools::Itertools;
        flush_buf_ack
            .iter()
            .zip_eq(worker_id_range.clone())
            .filter(|(ack, _)| !**ack)
            .for_each(|(_, id)| eprintln!("worker {id} fail to flush buf"));
        drop_ack
            .iter()
            .zip_eq(worker_id_range)
            .filter(|(ack, _)| !**ack)
            .for_each(|(_, id)| eprintln!("worker {id} fail to drop store"));
        println!("done");
        Ok(())
    }
}
