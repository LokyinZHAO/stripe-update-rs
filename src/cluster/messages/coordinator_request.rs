use bytes::Bytes;
use redis::Commands;
use serde::{Deserialize, Serialize};

use crate::{cluster::Ranges, storage::BlockId, SUResult};

use super::{
    payload::{PayloadData, PayloadID},
    TaskID,
};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: TaskID,
    pub head: Head,
    #[serde(skip) /*this filed this should be fetched from redis*/ ]
    pub payload: PayloadData,
}

impl Request {
    pub fn heartbeat() -> Self {
        Self::assemble(Head::HeartBeat, None)
    }

    pub fn shutdown() -> Self {
        Self::assemble(Head::Shutdown, None)
    }

    pub fn flush_buf() -> Self {
        Self::assemble(Head::FlushBuf, None)
    }

    pub fn drop_store() -> Self {
        Self::assemble(Head::DropStore, None)
    }

    pub fn store_block(id: BlockId, payload: Bytes) -> Self {
        Self::assemble(
            Head::StoreBlock {
                id,
                payload: PayloadID::assign(),
            },
            Some(payload),
        )
    }

    pub fn retrieve_slice(id: BlockId, ranges: Ranges) -> Self {
        Self::assemble(Head::RetrieveData { id, ranges }, None)
    }

    pub fn persist_update(id: BlockId) -> Self {
        Self::assemble(Head::PersistUpdate { id }, None)
    }

    pub fn buffer_update_data(id: BlockId, ranges: Ranges, payload: Bytes) -> Self {
        Self::assemble(
            Head::BufferUpdateData {
                id,
                ranges,
                payload: PayloadID::assign(),
            },
            Some(payload),
        )
    }

    pub fn update(id: BlockId, ranges: Ranges, payload: Bytes) -> Self {
        Self::assemble(
            Head::Update {
                id,
                ranges,
                payload: PayloadID::assign(),
            },
            Some(payload),
        )
    }
}

impl Request {
    fn assemble(head: Head, payload: Option<Bytes>) -> Self {
        if head.has_payload() {
            assert!(payload.is_some(), "payload is required");
        }
        Self {
            id: TaskID::assign(),
            head,
            payload: payload.map(PayloadData::new).unwrap_or_default(),
        }
    }

    pub fn push_to_redis(&self, conn: &mut redis::Connection, key: &str) -> SUResult<()> {
        // push payload
        if let Some(id) = self.head.get_payload_id() {
            self.payload.push_to_redis(id, conn)?;
        }
        let bin_ser = bincode::serialize(self).expect("serde error");
        Ok(conn.rpush(key, bin_ser)?)
    }

    pub fn fetch_from_redis(conn: &mut redis::Connection, key: &str) -> SUResult<Self> {
        let value: redis::Value = conn.blpop(key, 0_f64)?;
        if let redis::Value::Bulk(value) = value {
            let value = value.get(1).expect("bad redis value");
            if let redis::Value::Data(value) = value {
                let mut request: Request = bincode::deserialize(value).expect("serde error");
                if let Some(id) = request.head.get_payload_id() {
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
    #[allow(dead_code)]
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
            redis::Value::Data(value) => {
                let mut request: Request = bincode::deserialize(&value).expect("serde error");
                if let Some(id) = request.head.get_payload_id() {
                    request.payload = PayloadData::fetch_from_redis(id, conn)?;
                }
                Ok(Some(request))
            }
            _ => unreachable!("bad redis value"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone)]
/// Request(control flow) head sending from coordinator to workers
pub enum Head {
    /// Create a new block
    StoreBlock { id: BlockId, payload: PayloadID },
    /// Retrieve data from a block, and response with slice data
    RetrieveData { id: BlockId, ranges: Ranges },
    /// Persist buffered updates to hdd, and respond with update delta data
    PersistUpdate { id: BlockId },
    /// Buffer updates of a data block
    BufferUpdateData {
        id: BlockId,
        ranges: Ranges,
        payload: PayloadID,
    },
    /// Update block
    Update {
        id: BlockId,
        ranges: Ranges,
        payload: PayloadID,
    },
    /// Clean up all the buffers
    ///
    /// WARNING: this will cause data loss
    FlushBuf,
    /// Delete all the blocks
    ///
    /// WARNING: this will cause data loss
    DropStore,
    /// Heartbeat prober
    HeartBeat,
    /// Shutdown the worker
    Shutdown,
}

impl Head {
    pub fn has_payload(&self) -> bool {
        matches!(
            self,
            Self::StoreBlock { .. } | Self::BufferUpdateData { .. } | Self::Update { .. }
        )
    }

    pub fn get_payload_id(&self) -> Option<PayloadID> {
        match self {
            Self::StoreBlock { payload, .. } => Some(*payload),
            Self::BufferUpdateData { payload, .. } => Some(*payload),
            Self::Update { payload, .. } => Some(*payload),
            _ => None,
        }
    }
}
