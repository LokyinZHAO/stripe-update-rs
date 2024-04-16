use bytes::Bytes;
use redis::Commands;

use crate::SUResult;

use super::Uuid;

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
    pub fn new(data: Bytes) -> Self {
        Self(Some(data))
    }

    pub fn unwrap(self) -> Bytes {
        self.0.unwrap()
    }

    // pub fn as_ref(&self) -> Option<&Bytes> {
    //     self.0.as_ref()
    // }

    pub fn fetch_from_redis(id: PayloadID, conn: &mut redis::Connection) -> SUResult<Self> {
        let value: redis::Value = conn.get_del(id)?;
        let data = match value {
            redis::Value::Nil => {
                return Err(crate::SUError::other(format!(
                    "payload id: {} not found",
                    id.0
                )))
            }
            redis::Value::Data(data) => data.into(),
            _ => unreachable!("bad redis value"),
        };
        Ok(Self::new(data))
    }

    pub fn push_to_redis(&self, id: PayloadID, conn: &mut redis::Connection) -> SUResult<()> {
        let data = self.0.as_ref().unwrap().as_ref();
        // TODO: performance issue: redis makes a copy of the data
        let _: () = conn.set_options(
            id,
            data,
            redis::SetOptions::default().conditional_set(redis::ExistenceCheck::NX),
        )?;
        Ok(())
    }
}
