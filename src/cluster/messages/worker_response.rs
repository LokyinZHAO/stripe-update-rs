use redis::Commands;
use serde::{Deserialize, Serialize};

use crate::{
    cluster::{Ranges, WorkerID},
    SUResult,
};

use super::{PayloadData, PayloadID, TaskID};

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Copy)]
pub struct Nak(PayloadID);

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone)]
pub struct Response {
    pub id: TaskID,
    pub head: Result<Ack, Nak>,
    #[serde(skip) /*this filed this should be fetched from redis*/ ]
    pub payload: PayloadData,
}

impl Response {
    pub fn nak(task_id: TaskID, err: impl ToString) -> Self {
        Self::assemble_nak(task_id, err.to_string())
    }

    pub fn store_block(task_id: TaskID) -> Self {
        Self::assemble_ack(task_id, Ack::StoreBlock, None)
    }

    pub fn retrieve_slice(task_id: TaskID, payload: Vec<u8>) -> Self {
        Self::assemble_ack(
            task_id,
            Ack::RetrieveSlice {
                payload: PayloadID::assign(),
            },
            Some(payload),
        )
    }

    pub fn persist_update(task_id: TaskID, ranges: Ranges, payload: Vec<u8>) -> Self {
        Self::assemble_ack(
            task_id,
            Ack::PersistUpdate {
                ranges,
                payload: PayloadID::assign(),
            },
            Some(payload),
        )
    }

    pub fn buffer_update_data(task_id: TaskID) -> Self {
        Self::assemble_ack(task_id, Ack::BufferUpdateData, None)
    }

    pub fn update_parity(task_id: TaskID) -> Self {
        Self::assemble_ack(task_id, Ack::UpdateParity, None)
    }

    pub fn flush_buf(task_id: TaskID, worker_id: WorkerID) -> Self {
        Self::assemble_ack(task_id, Ack::FlushBuf { worker_id }, None)
    }

    pub fn drop_store(task_id: TaskID, worker_id: WorkerID) -> Self {
        Self::assemble_ack(task_id, Ack::DropStore { worker_id }, None)
    }

    pub fn heartbeat(task_id: TaskID, worker_id: WorkerID) -> Self {
        Self::assemble_ack(task_id, Ack::HeartBeat { worker_id }, None)
    }

    pub fn shutdown(task_id: TaskID, worker_id: WorkerID) -> Self {
        Self::assemble_ack(task_id, Ack::Shutdown { worker_id }, None)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum Ack {
    /// Create a new block
    StoreBlock,
    /// Retrieve data from a block, with slice data payload as response
    RetrieveSlice { payload: PayloadID },
    /// Persist buffered updates to hdd, with buffered updates payload as response
    PersistUpdate { ranges: Ranges, payload: PayloadID },
    /// Buffer Updates of a block
    BufferUpdateData,
    /// Update parity block
    UpdateParity,
    /// Clean up all the buffered slices
    FlushBuf { worker_id: WorkerID },
    /// Delete all the blocks
    DropStore { worker_id: WorkerID },
    /// Ack for Heartbeat
    HeartBeat { worker_id: WorkerID },
    /// Shutdown the worker
    Shutdown { worker_id: WorkerID },
}

impl Ack {
    fn has_payload(&self) -> bool {
        matches!(
            self,
            Self::RetrieveSlice { .. } | Self::PersistUpdate { .. }
        )
    }

    fn get_payload_id(&self) -> Option<PayloadID> {
        match self {
            Self::RetrieveSlice { payload, .. } => Some(*payload),
            Self::PersistUpdate { payload, .. } => Some(*payload),
            _ => None,
        }
    }
}

impl Response {
    fn assemble_ack(task_id: TaskID, head: Ack, payload: Option<Vec<u8>>) -> Self {
        if head.has_payload() {
            assert!(payload.is_some(), "payload is required");
        }
        Self {
            id: task_id,
            head: Ok(head),
            payload: PayloadData(payload),
        }
    }

    fn assemble_nak(task_id: TaskID, err_str: impl Into<String>) -> Self {
        Self {
            id: task_id,
            head: Err(Nak(PayloadID::assign())),
            payload: PayloadData::new(err_str.into().into_bytes()),
        }
    }

    pub fn push_to_redis(&self, conn: &mut redis::Connection, key: &str) -> SUResult<()> {
        if let Some(payload) = self.head.as_ref().ok().and_then(Ack::get_payload_id) {
            self.payload.push_to_redis(payload, conn)?;
        }
        let bin_ser = bincode::serialize(self).expect("serde error");
        Ok(conn.rpush(key, bin_ser)?)
    }

    pub fn fetch_from_redis(conn: &mut redis::Connection, key: &str) -> SUResult<Self> {
        let value: redis::Value = conn.blpop(key, 0_f64)?;
        if let redis::Value::Bulk(value) = value {
            let value = value.get(1).expect("bad redis value");
            if let redis::Value::Data(bin_ser) = value {
                let mut request: Response = bincode::deserialize(&bin_ser).expect("serde error");
                if let Some(id) = request.head.as_ref().ok().and_then(Ack::get_payload_id) {
                    request.payload = PayloadData::fetch_from_redis(id, conn)?;
                }
                return Ok(request);
            }
        }
        unreachable!("bad redis value")
    }

    /// Fetch a request from redis with timeout
    ///
    /// If timeout is None, it will never be blocked and return `None` when there is no request.
    pub fn fetch_from_redis_timeout(
        conn: &mut redis::Connection,
        key: &str,
        timeout: Option<std::time::Duration>,
    ) -> SUResult<Option<Self>> {
        let value: redis::Value = if let Some(timeout) = timeout {
            let timeout = timeout.as_secs_f64();
            conn.blpop(key, timeout)?
        } else {
            conn.lpop(key, None)?
        };
        match value {
            // timeout
            redis::Value::Nil => Ok(None),
            redis::Value::Data(bin_ser) => {
                let mut request: Response = bincode::deserialize(&bin_ser).expect("serde error");
                if let Some(id) = request.head.as_ref().ok().and_then(Ack::get_payload_id) {
                    request.payload = PayloadData::fetch_from_redis(id, conn)?;
                }
                Ok(Some(request))
            }
            _ => unreachable!("bad redis value"),
        }
    }
}
