use bytes::Bytes;
use redis::Commands;

use crate::SUResult;

pub mod coordinator_request;
pub mod worker_response;

#[derive(
    Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy,
)]
struct Uuid(#[serde(with = "uuid::serde::compact")] uuid::Uuid);

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_hyphenated().fmt(f)
    }
}

impl redis::ToRedisArgs for Uuid {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        self.0.as_bytes().write_redis_args(out)
    }
}

impl Uuid {
    fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy,
)]
pub struct TaskID(Uuid);

impl TaskID {
    pub(crate) fn assign() -> TaskID {
        TaskID(Uuid::new())
    }
}

impl std::fmt::Display for TaskID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Copy)]
pub struct PayloadID(Uuid);

impl redis::ToRedisArgs for PayloadID {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        self.0.write_redis_args(out)
    }
}

impl PayloadID {
    pub(crate) fn assign() -> PayloadID {
        PayloadID(Uuid::new())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Default)]
pub struct PayloadData(Option<Bytes>);

impl PayloadData {
    fn new(data: Bytes) -> Self {
        Self(Some(data))
    }

    pub fn unwrap(self) -> Bytes {
        self.0.unwrap()
    }

    pub fn fetch_from_redis(id: PayloadID, conn: &mut redis::Connection) -> SUResult<Self> {
        let value: redis::Value = conn.get_del(id)?;
        let data = match value {
            redis::Value::Nil => {
                return Err(crate::SUError::other(format!(
                    "payload id: {} not found",
                    id.0
                )))
            }
            redis::Value::Data(data) => data,
            _ => unreachable!("bad redis value"),
        };
        Ok(Self::new(data.into()))
    }

    pub fn push_to_redis(&self, id: PayloadID, conn: &mut redis::Connection) -> SUResult<()> {
        let data = self.0.as_ref().unwrap().as_ref();
        // TODO: performance issue: redis makes a copy of the data
        conn.set_options(
            id,
            data,
            redis::SetOptions::default().conditional_set(redis::ExistenceCheck::NX),
        )?;
        Ok(())
    }
}
