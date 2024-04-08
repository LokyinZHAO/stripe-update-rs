use std::num::NonZeroUsize;

use crate::{config, SUError, SUResult};
use itertools::Itertools;
use redis::{Commands, FromRedisValue};

mod bench_update;
mod build_data;
mod kill_all;
mod purge;
pub mod cmds {
    pub use super::build_data::BuildData;
    pub use super::kill_all::KillAll;
    pub use super::purge::Purge;
}

use super::{
    format_request_queue_key, format_response_queue_key,
    messages::{CoordinatorRequest, WorkerResponse},
    WorkerID,
};

#[derive(Debug, Clone, Default)]
pub struct CoordinatorBuilder {
    redis_url: Option<String>,
    block_size: Option<usize>,
    slice_size: Option<usize>,
    block_num: Option<usize>,
    worker_num: Option<usize>,
    k_p: Option<(usize, usize)>,
}

impl CoordinatorBuilder {
    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.redis_url = Some(url.into());
        self
    }

    pub fn block_size(mut self, size: NonZeroUsize) -> Self {
        self.block_size = Some(size.get());
        self
    }

    pub fn slice_size(mut self, size: NonZeroUsize) -> Self {
        self.slice_size = Some(size.get());
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
}

pub trait CoordinatorCmds {
    /// Execute coordinator command
    fn exec(self: Box<Self>) -> SUResult<()>;
}

/// Broadcasts a heartbeat message to all workers and waits for their responses.
///
/// # Returns
/// The alive workers' IDs.
fn broadcast_heartbeat(
    request_queue_list: &[impl redis::ToRedisArgs],
    response_queue: &impl redis::ToRedisArgs,
    conn: &mut redis::Connection,
) -> SUResult<Vec<WorkerID>> {
    request_queue_list
        .iter()
        .try_for_each(|key| conn.rpush(key, CoordinatorRequest::HeartBeat))
        .map_err(SUError::from)?;
    std::thread::sleep(config::heartbeat_interval());
    let worker_num = request_queue_list.len();
    let status = (0..worker_num)
        .map(|_| -> SUResult<_> {
            let res: redis::Value = conn.lpop(response_queue, None)?;
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
