use std::num::NonZeroUsize;

use crate::{config, SUError, SUResult};
use itertools::Itertools;
use redis::{Commands, FromRedisValue};

mod build_data;
mod kill_all;
mod purge;

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
    k_p: (usize, usize),
}

impl Coordinator {
    /// Broadcasts a heartbeat message to all workers and waits for their responses.
    ///
    /// # Returns
    /// The alive workers' IDs.
    fn broadcast_heartbeat(&self, conn: &mut redis::Connection) -> SUResult<Vec<WorkerID>> {
        self.config
            .request_queue_list
            .iter()
            .try_for_each(|key| conn.rpush(key, CoordinatorRequest::HeartBeat))
            .map_err(SUError::from)?;
        std::thread::sleep(config::heartbeat_interval());
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
        Ok(status.into_iter().flatten().dedup().sorted().collect())
    }
}
