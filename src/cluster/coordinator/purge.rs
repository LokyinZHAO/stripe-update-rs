use std::collections::BTreeMap;

use indicatif::ProgressIterator;

use crate::{
    cluster::{
        format_request_queue_key,
        messages::{
            coordinator_request::Request,
            worker_response::{Ack, Response},
        },
        progress_style_template, MessageQueueKey, WorkerID,
    },
    SUError, SUResult,
};

use super::CoordinatorCmds;

pub struct Purge {
    conn: redis::Connection,
    request_queue_list: Vec<MessageQueueKey>,
    response_queue: MessageQueueKey,
}

impl TryFrom<super::CoordinatorBuilder> for Purge {
    type Error = SUError;

    fn try_from(value: super::CoordinatorBuilder) -> Result<Self, Self::Error> {
        let redis_url = value
            .redis_url
            .ok_or_else(|| SUError::Other("redis url not set".into()))?;
        let worker_num = value
            .worker_num
            .ok_or_else(|| SUError::Other("worker number not set".into()))?;
        Ok(Purge {
            conn: redis::Client::open(redis_url)?.get_connection()?,
            request_queue_list: (1..=worker_num)
                .map(|i| i.try_into().unwrap())
                .map(WorkerID)
                .map(format_request_queue_key)
                .collect(),
            response_queue: crate::cluster::format_response_queue_key(),
        })
    }
}

impl CoordinatorCmds for Purge {
    fn exec(mut self: Box<Self>) -> SUResult<()> {
        let worker_num = self.request_queue_list.len();

        redis::cmd("FLUSHALL").query(&mut self.conn)?;

        // get alive workers
        let alive_workers = super::broadcast_heartbeat(
            &self.request_queue_list,
            &self.response_queue,
            &mut self.conn,
        )?;
        println!(
            "alive workers: {}",
            alive_workers
                .iter()
                .map(WorkerID::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );

        println!("purging dir...");
        let mut flush_tasks = self
            .request_queue_list
            .iter()
            .map(|key| -> SUResult<_> {
                let request = Request::flush_buf();
                let id = request.id;
                request
                    .push_to_redis(&mut self.conn, key)
                    .map(|_| (id, None::<Response>))
            })
            .collect::<SUResult<BTreeMap<_, _>>>()?;
        let mut drop_tasks = self
            .request_queue_list
            .iter()
            .map(|key| -> SUResult<_> {
                let request = Request::drop_store();
                let id = request.id;
                request
                    .push_to_redis(&mut self.conn, key)
                    .map(|_| (id, None::<Response>))
            })
            .collect::<SUResult<BTreeMap<_, _>>>()?;

        (0..worker_num * 2)
            .progress_with_style(progress_style_template(Some("purging worker data")))
            .try_for_each(|_| -> SUResult<()> {
                let response = Response::fetch_from_redis(&mut self.conn, &self.response_queue)?;
                let task_id = response.id;
                match &response.head {
                    Ok(Ack::FlushBuf { .. }) => {
                        flush_tasks
                            .get_mut(&task_id)
                            .expect("bad task id")
                            .replace(response);
                    }
                    Ok(Ack::DropStore { .. }) => {
                        drop_tasks
                            .get_mut(&task_id)
                            .expect("bad task id")
                            .replace(response);
                    }
                    Err(..) => {
                        flush_tasks
                            .get_mut(&task_id)
                            .or_else(|| drop_tasks.get_mut(&task_id))
                            .expect("bad task id")
                            .replace(response);
                    }
                    _ => unreachable!("bad response"),
                }
                Ok(())
            })?;
        assert!(flush_tasks
            .iter()
            .chain(drop_tasks.iter())
            .all(|(_, response)| { response.is_some() }));
        flush_tasks
            .into_iter()
            .filter(|(_, response)| response.as_ref().unwrap().head.is_err())
            .for_each(|(task_id, response)| {
                let response = response.unwrap();
                let err_str = String::from_utf8(response.payload.unwrap()).unwrap();
                eprintln!("flush task {} failed: {}", task_id, err_str);
            });
        drop_tasks
            .into_iter()
            .filter(|(_, response)| response.as_ref().unwrap().head.is_err())
            .for_each(|(task_id, response)| {
                let response = response.unwrap();
                let err_str = String::from_utf8(response.payload.unwrap()).unwrap();
                eprintln!("drop task {} failed: {}", task_id, err_str);
            });
        println!("done");
        Ok(())
    }
}
