use crate::{
    cluster::{
        format_request_queue_key,
        messages::{CoordinatorRequest, WorkerResponse},
        MessageQueueKey, WorkerID,
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
                .map(WorkerID)
                .map(format_request_queue_key)
                .collect(),
            response_queue: super::format_response_queue_key(),
        })
    }
}

impl super::CoordinatorCmds for KillAll {
    fn exec(mut self: Box<Self>) -> SUResult<()> {
        // let mut conn = self
        //     .client
        //     .get_connection()
        //     .expect("fail to get redis connection");
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
        alive_workers
            .iter()
            .map(|worker_id| format_request_queue_key(*worker_id))
            .try_for_each(|key| {
                CoordinatorRequest::Shutdown.try_push_to_redis(&mut self.conn, &key)
            })?;
        println!("\nwaiting for workers to shutdown...");
        std::io::stdout().flush().unwrap();
        (0..alive_workers.len())
            .try_for_each(|_| {
                let res: WorkerResponse =
                    WorkerResponse::try_fetch_from_redis(&mut self.conn, &self.response_queue)?;
                match res {
                    WorkerResponse::Shutdown(WorkerID(id)) => {
                        println!("worker {id} has been shutdown")
                    }
                    WorkerResponse::Nak(err) => eprintln!("shutdown fails: {err}"),
                    _ => panic!("bad response"),
                }
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
