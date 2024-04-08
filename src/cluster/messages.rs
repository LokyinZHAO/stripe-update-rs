use serde::{Deserialize, Serialize};

use crate::{storage::BlockId, SUResult};

use super::{Ranges, WorkerID};

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone)]
/// Request(control flow) sending from coordinator to workers
pub enum CoordinatorRequest {
    /// Create a new block
    StoreBlock { id: BlockId, payload: Vec<u8> },
    /// Retrieve data from a block, and response with slice data
    RetrieveData { id: BlockId, ranges: Ranges },
    /// Persist buffered updates to hdd, and respond with buffered data
    PersistUpdate { id: BlockId },
    /// Buffer updates of a data block
    BufferUpdateData {
        id: BlockId,
        ranges: Ranges,
        payload: Vec<u8>,
    },
    /// Update parity block
    UpdateParity {
        id: BlockId,
        ranges: Ranges,
        payload: Vec<u8>,
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

impl CoordinatorRequest {
    pub(crate) fn try_push_to_redis(
        &self,
        conn: &mut redis::Connection,
        key: &str,
    ) -> SUResult<()> {
        use redis::Commands;
        Ok(conn.rpush(key, self)?)
    }

    pub(crate) fn try_fetch_from_redis(conn: &mut redis::Connection, key: &str) -> SUResult<Self> {
        use redis::Commands;
        let value: redis::Value = conn.blpop(key, 0_f64)?;
        if let redis::Value::Bulk(values) = value {
            Ok(redis::from_redis_value(&values[1])?)
        } else {
            unreachable!("bad redis value")
        }
    }
}

impl redis::ToRedisArgs for CoordinatorRequest {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let bin_ser = bincode::serialize(self).expect("serde error");
        bin_ser.write_redis_args(out)
    }
}

impl redis::FromRedisValue for CoordinatorRequest {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let bin_ser: Vec<u8> = redis::from_redis_value(v)?;
        Ok(bincode::deserialize(&bin_ser).expect("serde error"))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerResponse {
    /// Create a new block
    StoreBlock,
    /// Retrieve data from a block, with slice data payload as response
    RetrieveSlice(Vec<u8>),
    /// Persist buffered updates to hdd, with buffered updates payload as response
    PersistUpdate(Ranges, Vec<u8>),
    /// Buffer Updates of a block
    BufferUpdateData,
    /// Update parity block
    UpdateParity,
    /// Clean up all the buffered slices
    FlushBuf(WorkerID),
    /// Delete all the blocks
    DropStore(WorkerID),
    /// Ack for Heartbeat
    HeartBeat(WorkerID),
    /// Shutdown the worker
    Shutdown(WorkerID),
    /// Error occurs
    Nak(String),
}

impl WorkerResponse {
    pub(crate) fn try_push_to_redis(
        &self,
        conn: &mut redis::Connection,
        key: &str,
    ) -> SUResult<()> {
        use redis::Commands;
        Ok(conn.rpush(key, self)?)
    }

    pub(crate) fn try_fetch_from_redis(conn: &mut redis::Connection, key: &str) -> SUResult<Self> {
        use redis::Commands;
        let value: redis::Value = conn.blpop(key, 0_f64)?;
        if let redis::Value::Bulk(values) = value {
            Ok(redis::from_redis_value(&values[1])?)
        } else {
            unreachable!("bad redis value")
        }
    }
}

impl redis::ToRedisArgs for WorkerResponse {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let bin_ser = bincode::serialize(self).expect("serde error");
        bin_ser.write_redis_args(out)
    }
}

impl redis::FromRedisValue for WorkerResponse {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let bin_ser: Vec<u8> = redis::from_redis_value(v)?;
        Ok(bincode::deserialize(&bin_ser).expect("serde error"))
    }
}
