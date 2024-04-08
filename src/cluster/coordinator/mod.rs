use std::num::NonZeroUsize;

use crate::{SUError, SUResult};
use redis::{Commands, FromRedisValue};

mod build_data;

use super::{
    format_request_queue_key, format_response_queue_key,
    messages::{CoordinatorRequest, WorkerResponse},
    MessageQueueKey, WorkerID,
};

#[derive(Debug, Clone, Default)]
pub struct CoordinatorBuilder {
    redis_url: String,
    block_size: Option<usize>,
    block_num: Option<usize>,
    purge: bool,
    worker_num: Option<usize>,
    k_p: Option<(usize, usize)>,
}

impl CoordinatorBuilder {
    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.redis_url = url.into();
        self
    }

    pub fn block_size(mut self, size: NonZeroUsize) -> Self {
        self.block_size = Some(size.get());
        self
    }

    pub fn block_num(mut self, num: NonZeroUsize) -> Self {
        self.block_num = Some(num.get());
        self
    }

    pub fn purge(mut self, purge: bool) -> Self {
        self.purge = purge;
        self
    }

    pub fn worker_num(mut self, num: NonZeroUsize) -> Self {
        self.worker_num = Some(num.get());
        self
    }

    pub fn k_p(mut self, k: NonZeroUsize, p: NonZeroUsize) -> Self {
        self.k_p = Some((k.get(), p.get()));
        self
    }

    pub fn build(self) -> SUResult<Coordinator> {
        let client = redis::Client::open(self.redis_url)?;
        Ok(Coordinator {
            client,
            config: ConfigParam {
                request_queue_list: (1..=self.worker_num.expect("worker num not set"))
                    .map(|id| format_request_queue_key(WorkerID(id)))
                    .collect(),
                response_queue: format_response_queue_key(),
                block_size: self.block_size.expect("block size not set"),
                block_num: self.block_num.expect("block num not set"),
                purge: self.purge,
                k_p: self.k_p.expect("k and p not set"),
            },
        })
    }
}

pub struct Coordinator {
    client: redis::Client,
    config: ConfigParam,
}

struct ConfigParam {
    request_queue_list: Vec<MessageQueueKey>,
    response_queue: MessageQueueKey,
    block_size: usize,
    block_num: usize,
    purge: bool,
    k_p: (usize, usize),
}

impl Coordinator {
    /// Broadcasts a heartbeat message to all workers and waits for their responses.
    ///
    /// # Returns
    /// The alive workers' IDs.
    fn broadcast_heartbeat(&self, conn: &mut redis::Connection) -> SUResult<Vec<WorkerID>> {
        const WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
        self.config
            .request_queue_list
            .iter()
            .try_for_each(|key| conn.rpush(key, CoordinatorRequest::HeartBeat))
            .map_err(SUError::from)?;
        std::thread::sleep(WAIT_TIMEOUT);
        let worker_num = self.config.request_queue_list.len();
        let status = (0..worker_num)
            .map(|_| -> SUResult<_> {
                let res: redis::Value = conn.lpop(&self.config.response_queue, None)?;
                if let redis::Value::Nil = res {
                    Ok(None)
                } else {
                    let res: WorkerResponse = WorkerResponse::from_redis_value(&res)?;
                    Ok(match res {
                        WorkerResponse::HeartBeat(id) => Some(id),
                        _ => unreachable!("bad response"),
                    })
                }
            })
            .collect::<Result<Vec<_>, SUError>>()?;
        Ok(status.into_iter().flatten().collect())
    }

    pub fn kill_all(self) -> SUResult<()> {
        let mut conn = self
            .client
            .get_connection()
            .expect("fail to get redis connection");
        redis::cmd("FLUSHALL").query(&mut conn)?;
        println!("broadcasting heartbeat...");
        std::io::stdout().flush().unwrap();
        let alive_workers = self.broadcast_heartbeat(&mut conn)?;
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
            .try_for_each(|key| CoordinatorRequest::Shutdown.try_push_to_redis(&mut conn, &key))?;
        println!("\nwaiting for workers to shutdown...");
        std::io::stdout().flush().unwrap();
        (0..alive_workers.len())
            .try_for_each(|_| {
                let res: WorkerResponse =
                    WorkerResponse::try_fetch_from_redis(&mut conn, &self.config.response_queue)?;
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
        redis::cmd("FLUSHALL").query(&mut conn)?;
        println!("done!");
        Ok(())
    }
}
