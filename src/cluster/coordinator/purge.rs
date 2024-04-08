use std::io::Write;

use redis::Commands;

use crate::{
    cluster::{
        messages::{CoordinatorRequest, WorkerResponse},
        WorkerID,
    },
    SUError, SUResult,
};

use super::Coordinator;

impl Coordinator {
    pub fn purge(self) -> SUResult<()> {
        let request_list = self.config.request_queue_list.clone();
        let worker_id_range = 1..request_list.len() + 1;
        let response_list = self.config.response_queue.clone();

        // connect to redis
        let mut conn = self.client.get_connection()?;
        // get alive workers
        let alive_workers = self.broadcast_heartbeat(&mut conn)?;
        println!(
            "alive workers: {}",
            alive_workers
                .iter()
                .map(WorkerID::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
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
        worker_id_range.clone().try_for_each(|_| -> SUResult<()> {
            let flush_response = WorkerResponse::try_fetch_from_redis(&mut conn, &response_list)?;
            match flush_response {
                WorkerResponse::FlushBuf => (),
                WorkerResponse::Nak(err) => {
                    return Err(SUError::Other(format!("worker error: {err}")))
                }
                _ => unreachable!("unexpected response"),
            }
            let drop_response = WorkerResponse::try_fetch_from_redis(&mut conn, &response_list)?;
            match drop_response {
                WorkerResponse::DropStore => (),
                WorkerResponse::Nak(err) => {
                    return Err(SUError::Other(format!("worker error: {err}")))
                }
                _ => unreachable!("unexpected response"),
            }
            Ok(())
        })?;
        println!("done");
        Ok(())
    }
}
