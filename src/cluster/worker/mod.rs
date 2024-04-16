use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::mpsc::{Receiver, SyncSender},
};

use bytes::{Bytes, BytesMut};

use crate::{
    cluster::dev_display,
    storage::{
        BlockId, BlockStorage, EvictStrategySlice, FixedSizeSliceBuf, HDDStorage, NonEvict,
        SliceBuffer, SliceStorage,
    },
    SUError, SUResult,
};

use super::{
    format_request_queue_key, format_response_queue_key,
    messages::{
        coordinator_request::{Head as RequestHead, Request},
        worker_response::Response,
        TaskID,
    },
    Ranges, WorkerID,
};

#[derive(Debug, Default, Clone)]
pub struct WorkerBuilder {
    id: Option<WorkerID>,
    client: Option<redis::Client>,
    queue_key: Option<(String, String)>,
    hdd_dev_path: Option<PathBuf>,
    ssd_dev_path: Option<PathBuf>,
    block_size: Option<NonZeroUsize>,
}

impl WorkerBuilder {
    pub fn id(&mut self, id: usize) -> &mut Self {
        self.id = Some(WorkerID(id.try_into().unwrap()));
        self.queue_key = Some((
            format_request_queue_key(WorkerID(id.try_into().unwrap())),
            format_response_queue_key(),
        ));
        self
    }

    pub fn client(&mut self, url: impl redis::IntoConnectionInfo) -> &mut Self {
        self.client = Some(redis::Client::open(url).expect("invalid redis url"));
        self
    }

    pub fn ssd_dev_path(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.ssd_dev_path = Some(path.into());
        self
    }

    pub fn hdd_dev_path(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.hdd_dev_path = Some(path.into());
        self
    }

    pub fn block_size(&mut self, size: NonZeroUsize) -> &mut Self {
        self.block_size = Some(size);
        self
    }

    pub fn work(&self) -> SUResult<()> {
        Worker::try_from(self.to_owned())?.work()
    }
}

struct Worker {
    id: WorkerID,
    client: redis::Client,
    request_queue_key: String,
    response_queue_key: String,
    ssd_dev_path: PathBuf,
    hdd_dev_path: PathBuf,
    block_size: usize,
}

impl Worker {
    fn work(self) -> SUResult<()> {
        const CH_SIZE: usize = 16;
        const GET_CONNECTION_ERR_STR: &str = "fail to get redis connection";
        let recv_conn = self.client.get_connection().expect(GET_CONNECTION_ERR_STR);
        let send_conn = self.client.get_connection().expect(GET_CONNECTION_ERR_STR);
        let hdd_dev = HDDStorage::connect_to_dev(
            &self.hdd_dev_path,
            NonZeroUsize::new(self.block_size).unwrap(),
        )?;
        let slice_buf = FixedSizeSliceBuf::connect_to_dev_with_evict(
            &self.ssd_dev_path,
            NonZeroUsize::new(self.block_size).unwrap(),
            NonEvict::default(),
        )
        .unwrap();
        let (request_send, request_recv) = std::sync::mpsc::sync_channel(CH_SIZE);
        let (response_send, response_recv) = std::sync::mpsc::sync_channel(CH_SIZE);
        println!("worker id: {}", self.id.0);
        println!("ssd device path: {}", dev_display(&self.ssd_dev_path));
        println!("hdd device path: {}", dev_display(&self.hdd_dev_path));
        println!("request queue key: {}", self.request_queue_key);
        println!("response queue key: {}", self.response_queue_key);
        println!("block size: {}", self.block_size);
        println!("start working...");

        let recv_handle = std::thread::spawn(move || {
            receiver_thread_handle(recv_conn, self.request_queue_key, request_send)
        });
        let work_handle = std::thread::spawn(move || {
            worker_thread_handle(self.id, request_recv, response_send, hdd_dev, slice_buf)
        });
        let send_handle = std::thread::spawn(move || {
            sender_thread_handle(send_conn, self.response_queue_key, response_recv)
        });

        recv_handle.join().expect("thread join error").unwrap();
        work_handle.join().expect("thread join error").unwrap();
        send_handle.join().expect("thread join error").unwrap();
        Ok(())
    }
}

impl TryFrom<WorkerBuilder> for Worker {
    type Error = SUError;

    fn try_from(value: WorkerBuilder) -> Result<Self, Self::Error> {
        let (request, response) = value
            .queue_key
            .ok_or_else(|| SUError::Other("queue keys not set".into()))?;
        Ok(Worker {
            id: value
                .id
                .ok_or_else(|| SUError::Other("worker id not set".into()))?,
            client: value
                .client
                .ok_or_else(|| SUError::Other("redis client not set".into()))?,
            request_queue_key: request,
            response_queue_key: response,
            ssd_dev_path: value
                .ssd_dev_path
                .ok_or_else(|| SUError::Other("ssd device path not set".into()))?,
            hdd_dev_path: value
                .hdd_dev_path
                .ok_or_else(|| SUError::Other("hdd device path not set".into()))?,
            block_size: value
                .block_size
                .ok_or_else(|| SUError::Other("block size not set".into()))?
                .get(),
        })
    }
}

fn receiver_thread_handle(
    mut conn: redis::Connection,
    key: String,
    ch: SyncSender<Request>,
) -> SUResult<()> {
    let mut shutdown = false;
    while !shutdown {
        let request = Request::fetch_from_redis(&mut conn, &key)?;
        shutdown = matches!(&request.head, RequestHead::Shutdown);
        ch.send(request)
            .expect("bad mpsc: all the consumers are disconnected");
    }
    Ok(())
}

fn sender_thread_handle(
    mut conn: redis::Connection,
    key: String,
    ch: Receiver<Response>,
) -> SUResult<()> {
    while let Ok(response) = ch.recv() {
        response.push_to_redis(&mut conn, &key)?;
    }
    Ok(())
}

fn worker_thread_handle(
    worker_id: WorkerID,
    recv_ch: Receiver<Request>,
    send_ch: SyncSender<Response>,
    mut hdd_store: HDDStorage,
    mut ssd_buf: FixedSizeSliceBuf<NonEvict>,
) -> SUResult<()> {
    while let Ok(Request {
        id: task_id,
        head,
        payload,
    }) = recv_ch.recv()
    {
        let response = match head {
            RequestHead::StoreBlock { id, .. } => {
                do_store_block(task_id, &mut hdd_store, id, payload.unwrap())
            }
            RequestHead::RetrieveData { id, ranges } => {
                do_retrieve_data(task_id, &mut hdd_store, id, ranges)
            }
            RequestHead::PersistUpdate { id } => {
                do_persist_update(task_id, &mut hdd_store, &mut ssd_buf, id)
            }
            RequestHead::BufferUpdateData { id, ranges, .. } => {
                do_buffer_update_data(task_id, &mut ssd_buf, id, ranges, payload.unwrap())
            }
            RequestHead::Update { id, ranges, .. } => {
                do_update(task_id, &mut hdd_store, id, ranges, payload.unwrap())
            }
            RequestHead::FlushBuf => do_flush_buf(task_id, worker_id, &mut ssd_buf),
            RequestHead::DropStore => do_drop_store(task_id, worker_id, &mut hdd_store),
            RequestHead::HeartBeat => do_heartbeat(task_id, worker_id),
            RequestHead::Shutdown => do_shutdown(task_id, worker_id),
        }?;
        send_ch.send(response).unwrap();
    }
    Ok(())
}

fn do_store_block(
    task_id: TaskID,
    hdd_store: &mut HDDStorage,
    block_id: BlockId,
    data: Bytes,
) -> SUResult<Response> {
    Ok(hdd_store
        .put_block(block_id, &data)
        .map(|()| Response::store_block(task_id))
        .unwrap_or_else(|e| Response::nak(task_id, e)))
}

fn do_retrieve_data(
    task_id: TaskID,
    hdd_store: &mut HDDStorage,
    block_id: BlockId,
    ranges: Ranges,
) -> SUResult<Response> {
    let mut data = BytesMut::zeroed(ranges.len());
    let mut cursor = 0;
    for range in ranges.to_ranges().iter() {
        let len = range.len();
        match hdd_store.get_slice(block_id, cursor, &mut data[cursor..cursor + len]) {
            Ok(Some(_)) => {
                cursor += len;
            }
            Ok(None) => {
                return Ok(Response::nak(
                    task_id,
                    format!("block {block_id} not found"),
                ));
            }
            Err(SUError::Range(range_err)) => {
                return Ok(Response::nak(task_id, format!("range error: {range_err}")));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(Response::retrieve_slice(task_id, data.freeze()))
}

fn do_persist_update(
    task_id: TaskID,
    hdd_store: &mut HDDStorage,
    ssd_buf: &mut FixedSizeSliceBuf<impl EvictStrategySlice>,
    block_id: BlockId,
) -> SUResult<Response> {
    log::debug!("persist update for block {block_id}");
    let response = ssd_buf.pop_one(block_id);
    if response.is_none() {
        log::debug!("buffer for block id {block_id} not found");
        return Ok(Response::nak(
            task_id,
            format!("buffer slice for block {block_id} not found"),
        ));
    }
    let eviction = response.unwrap();
    let mut ranges = Ranges::empty();
    let mut cursor = 0;
    let result = eviction
        .data
        .slices
        .into_iter()
        .filter_map(|slice| match slice {
            crate::storage::SliceOpt::Present(data) => {
                let range = cursor..cursor + data.len();
                ranges
                    .0
                    .intersection_with(&range_collections::RangeSet2::from(range.clone()));
                cursor += data.len();
                Some((data, range))
            }
            crate::storage::SliceOpt::Absent(size) => {
                cursor += size;
                None
            }
        })
        .map(|(data, range)| {
            hdd_store
                .put_slice(block_id, range.start, &data)
                .map_err(|e| Response::nak(task_id, format!("fail to persist updates: {e}")))
                .and_then(|opt| {
                    opt.map(|_| data).ok_or_else(|| {
                        Response::nak(task_id, format!("block {block_id} not found"))
                    })
                })
        })
        .collect::<Result<Vec<_>, Response>>()
        .map(|bytes| /* WARNING: flatten may cause vec memory reallocation */ bytes.into_iter().flatten().collect::<Bytes>())
        .map(|data| Response::persist_update(task_id, ranges, data))
        .unwrap_or_else(std::convert::identity);
    Ok(result)
}

fn do_buffer_update_data(
    task_id: TaskID,
    ssd_buf: &mut FixedSizeSliceBuf<impl EvictStrategySlice>,
    block_id: BlockId,
    ranges: Ranges,
    data: Bytes,
) -> SUResult<Response> {
    log::debug!("buffer update for block id {block_id}");
    let mut cursor = 0;
    for range in ranges.to_ranges().iter() {
        let update_slice = &data[cursor..cursor + range.len()];
        let result = ssd_buf.push_slice(block_id, range.start, update_slice);
        cursor += range.len();
        match result {
            Ok(Some(_)) => unreachable!("unexpected eviction"),
            Ok(None) => (),
            Err(SUError::Range(e)) => {
                return Ok(Response::nak(task_id, format!("range error: {e}")));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(Response::buffer_update_data(task_id))
}

fn do_update(
    task_id: TaskID,
    hdd_store: &mut HDDStorage,
    id: BlockId,
    ranges: Ranges,
    data: Bytes,
) -> SUResult<Response> {
    let mut cursor = 0;
    for range in ranges.to_ranges().iter() {
        // let slice_data = &data[cursor..cursor + range.len()];
        let slice_data = data.get(cursor..cursor + range.len());
        if slice_data.is_none() {
            let e = SUError::out_of_range(
                (file!(), line!(), column!()),
                Some(0..data.len()),
                cursor..cursor + range.len(),
            );
            return Ok(Response::nak(
                task_id,
                format!("fail to update block {id}: {e}"),
            ));
        }
        let slice_data = slice_data.unwrap();
        let result = hdd_store.put_slice(id, range.start, slice_data);
        cursor += range.len();
        match result {
            Ok(Some(_)) => (),
            Ok(None) => {
                return Ok(Response::nak(task_id, format!("block id {id} not found")));
            }
            Err(SUError::Range(e)) => {
                return Ok(Response::nak(task_id, format!("range error: {e}")));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(Response::update(task_id))
}

fn do_flush_buf(
    task_id: TaskID,
    worker_id: WorkerID,
    ssd_buf: &mut FixedSizeSliceBuf<impl EvictStrategySlice>,
) -> SUResult<Response> {
    Ok(ssd_buf
        .cleanup_dev()
        .map(|_| Response::flush_buf(task_id, worker_id))
        .unwrap_or_else(|e| Response::nak(task_id, format!("fail to flush buffer: {e}"))))
}

fn do_drop_store(
    task_id: TaskID,
    worker_id: WorkerID,
    hdd_store: &mut HDDStorage,
) -> SUResult<Response> {
    fn purge_dir(path: &std::path::Path) -> SUResult<()> {
        use std::fs;
        for entry in fs::read_dir(path)? {
            fs::remove_dir_all(entry?.path())?;
        }
        Ok(())
    }
    let dev_path = hdd_store.get_dev_root();
    let response = purge_dir(dev_path)
        .and_then(|_| std::fs::create_dir_all(dev_path).map_err(SUError::Io))
        .map(|_| Response::drop_store(task_id, worker_id))
        .unwrap_or_else(|e| Response::nak(task_id, format!("fail to drop store: {e}")));
    Ok(response)
}

fn do_heartbeat(task_id: TaskID, worker_id: WorkerID) -> SUResult<Response> {
    Ok(Response::heartbeat(task_id, worker_id))
}

fn do_shutdown(task_id: TaskID, worker_id: WorkerID) -> SUResult<Response> {
    Ok(Response::shutdown(task_id, worker_id))
}
