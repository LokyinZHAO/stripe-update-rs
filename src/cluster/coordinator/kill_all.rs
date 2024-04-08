use crate::{
    cluster::{
        format_request_queue_key,
        messages::{CoordinatorRequest, WorkerResponse},
        WorkerID,
    },
    SUError, SUResult,
};

use super::Coordinator;

impl Coordinator {
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
