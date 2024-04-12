use std::collections::BTreeMap;

use indicatif::ProgressIterator;

use crate::{
    cluster::{
        messages::{coordinator_request::Request, worker_response::Response},
        progress_style_template, MessageQueueKey, WorkerID,
    },
    SUError, SUResult,
};

pub struct KillAll {
    conn: redis::Connection,
    request_queue_list: Vec<MessageQueueKey>,
    response_queue: MessageQueueKey,
}

impl TryFrom<super::CoordinatorBuilder> for KillAll {
    type Error = SUError;

    fn try_from(value: super::CoordinatorBuilder) -> Result<Self, Self::Error> {
        let redis_url = value
            .redis_url
            .ok_or_else(|| SUError::Other("redis url not set".into()))?;
        let worker_num = value
            .worker_num
            .ok_or_else(|| SUError::Other("worker number not set".into()))?;
        Ok(KillAll {
            conn: redis::Client::open(redis_url)?.get_connection()?,
            request_queue_list: (1..=worker_num)
                .map(|i| i.try_into().unwrap())
                .map(WorkerID)
                .map(crate::cluster::format_request_queue_key)
                .collect(),
            response_queue: crate::cluster::format_response_queue_key(),
        })
    }
}

impl super::CoordinatorCmds for KillAll {
    fn exec(mut self: Box<Self>) -> SUResult<()> {
        redis::cmd("FLUSHALL").query(&mut self.conn)?;
        println!("broadcasting heartbeat...");
        std::io::stdout().flush().unwrap();
        let alive_workers = super::broadcast_heartbeat(
            &self.request_queue_list,
            &self.response_queue,
            &mut self.conn,
        )?;
        if alive_workers.is_empty() {
            println!("no worker is alive");
            return Ok(());
        }
        print!("alive workers:");
        alive_workers.iter().for_each(|&id| print!(" {id}"));
        use std::io::Write;
        std::io::stdout().flush().unwrap();
        let mut task_map = alive_workers
            .iter()
            .cloned()
            .map(crate::cluster::format_request_queue_key)
            .map(|key| {
                let request = Request::shutdown();
                let id = request.id;
                request
                    .push_to_redis(&mut self.conn, key.as_str())
                    .map(|_| (id, None))
            })
            .collect::<SUResult<BTreeMap<_, _>>>()?;
        println!("\nwaiting for workers to shutdown...");
        (0..alive_workers.len())
            .progress_with_style(progress_style_template(Some("shutting down workers")))
            .try_for_each(|_| {
                let res = Response::fetch_from_redis(&mut self.conn, &self.response_queue)?;
                task_map
                    .get_mut(&res.id)
                    .expect("unexpected response")
                    .replace(Some(res));
                Ok::<(), SUError>(())
            })
            .unwrap_or_else(|e| eprintln!("shutdown fails: {e}"));
        println!("done!");
        print!("flushing redis...");
        std::io::stdout().flush().unwrap();
        redis::cmd("FLUSHALL").query(&mut self.conn)?;
        println!("done!");
        Ok(())
    }
}
