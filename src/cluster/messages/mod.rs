pub mod coordinator_request;
pub mod payload;
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
        self.0.write_redis_args(out)
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
