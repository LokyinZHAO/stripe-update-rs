use std::{collections::BTreeMap, num::NonZeroUsize};

use itertools::Itertools;

use crate::{config, SUError, SUResult};

mod bench_update;
mod build_data;
mod kill_all;
mod purge;
pub mod cmds {
    pub use super::bench_update::BenchUpdate;
    pub use super::build_data::BuildData;
    pub use super::kill_all::KillAll;
    pub use super::purge::Purge;
}

use super::{
    messages::{
        coordinator_request::Request,
        worker_response::{Ack, Response},
        TaskID,
    },
    WorkerID,
};

#[derive(Debug, Clone, Default)]
pub struct CoordinatorBuilder {
    redis_url: Option<String>,
    block_size: Option<usize>,
    slice_size: Option<usize>,
    block_num: Option<usize>,
    buf_capacity: Option<usize>,
    worker_num: Option<usize>,
    k_p: Option<(usize, usize)>,
    test_load: Option<usize>,
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

    pub fn buf_capacity(mut self, size: NonZeroUsize) -> Self {
        self.buf_capacity = Some(size.get());
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

    pub fn test_load(mut self, test_load: NonZeroUsize) -> Self {
        self.test_load = Some(test_load.get());
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
    request_queue_list: &[impl AsRef<str>],
    response_queue: &impl AsRef<str>,
    conn: &mut redis::Connection,
) -> SUResult<Vec<WorkerID>> {
    let mut response_map = request_queue_list
        .iter()
        .map(|key| -> Result<TaskID, SUError> {
            let request = Request::heartbeat();
            let id = request.id;
            request.push_to_redis(conn, key.as_ref()).map(|_| id)
        })
        .map(|t_id| t_id.map(|id| (id, None)))
        .collect::<SUResult<BTreeMap<_, _>>>()?;
    std::thread::sleep(config::heartbeat_interval());
    let worker_num = request_queue_list.len();
    for _ in 0..worker_num {
        let response = Response::fetch_from_redis_timeout(conn, response_queue.as_ref(), None)?;
        if response.is_none() {
            // timeout
            break;
        }
        let response = response.unwrap();
        let id = response.id;
        let val = response_map.get_mut(&id).expect("bad response id");
        *val = Some(response);
    }
    let res = response_map
        .into_iter()
        .filter_map(|(_, v)| v)
        .filter_map(|response| match &response.head {
            Ok(Ack::HeartBeat { worker_id }) => Some(*worker_id),
            _ => None,
        })
        .sorted()
        .collect();
    Ok(res)
}
