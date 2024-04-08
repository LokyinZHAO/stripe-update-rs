use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::mpsc::{Receiver, SyncSender},
};

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
    messages::{CoordinatorRequest, WorkerResponse},
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
        self.id = Some(WorkerID(id));
        self.queue_key = Some((
            format_request_queue_key(WorkerID(id)),
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
    ch: SyncSender<CoordinatorRequest>,
) -> SUResult<()> {
    let mut shutdown = false;
    while !shutdown {
        let request = CoordinatorRequest::try_fetch_from_redis(&mut conn, &key)?;
        shutdown = matches!(request, CoordinatorRequest::Shutdown);
        ch.send(request).expect("bad mpsc: no consumer");
    }
    Ok(())
}

fn sender_thread_handle(
    mut conn: redis::Connection,
    key: String,
    ch: Receiver<WorkerResponse>,
) -> SUResult<()> {
    while let Ok(respond) = ch.recv() {
        respond.try_push_to_redis(&mut conn, &key)?;
    }
    Ok(())
}

fn worker_thread_handle(
    worker_id: WorkerID,
    recv_ch: Receiver<CoordinatorRequest>,
    send_ch: SyncSender<WorkerResponse>,
    mut hdd_store: HDDStorage,
    mut ssd_buf: FixedSizeSliceBuf<NonEvict>,
) -> SUResult<()> {
    while let Ok(request) = recv_ch.recv() {
        let shutdown = matches!(request, CoordinatorRequest::Shutdown);
        let response = match request {
            CoordinatorRequest::StoreBlock { id, payload } => {
                do_store_block(&mut hdd_store, id, payload)
            }
            CoordinatorRequest::RetrieveData { id, ranges } => {
                do_retrieve_data(&mut hdd_store, id, ranges)
            }
            CoordinatorRequest::PersistUpdate { id } => {
                do_persist_update(&mut hdd_store, &mut ssd_buf, id)
            }
            CoordinatorRequest::BufferUpdateData {
                id,
                ranges,
                payload,
            } => do_buffer_update_data(&mut ssd_buf, id, ranges, payload),
            CoordinatorRequest::UpdateParity {
                id,
                ranges,
                payload,
            } => do_update_parity(&mut hdd_store, id, ranges, payload),
            CoordinatorRequest::FlushBuf => do_flush_buf(&mut ssd_buf),
            CoordinatorRequest::DropStore => do_drop_store(&mut hdd_store),
            CoordinatorRequest::HeartBeat => do_heartbeat(worker_id),
            CoordinatorRequest::Shutdown => do_shutdown(worker_id),
        }?;
        send_ch.send(response).unwrap();
        if shutdown {
            println!("received shutdown signal from coordinator");
            break;
        }
    }
    Ok(())
}

fn do_store_block(
    hdd_store: &mut HDDStorage,
    id: BlockId,
    data: Vec<u8>,
) -> SUResult<WorkerResponse> {
    Ok(hdd_store
        .put_block(id, &data)
        .map(|()| WorkerResponse::StoreBlock)
        .unwrap_or_else(|e| WorkerResponse::Nak(e.to_string())))
}

fn do_retrieve_data(
    hdd_store: &mut HDDStorage,
    id: BlockId,
    ranges: Ranges,
) -> SUResult<WorkerResponse> {
    let mut data = vec![0_u8; ranges.len()];
    let mut cursor = 0;
    let res = ranges.to_ranges().iter().try_for_each(|range| {
        let len = range.len();
        match hdd_store.get_slice(id, cursor, &mut data[cursor..cursor + len]) {
            Ok(Some(_)) => {
                cursor += len;
                Ok(())
            }
            Ok(None) => Err(Ok(WorkerResponse::Nak(format!("block {id} not found")))),
            Err(SUError::Range(range_err)) => {
                Err(Ok(WorkerResponse::Nak(format!("range error: {range_err}"))))
            }
            Err(e) => Err(Err(e)),
        }
    });
    match res {
        Ok(_) => Ok(WorkerResponse::RetrieveSlice(data)),
        Err(Ok(nak_response)) => Ok(nak_response),
        Err(Err(e)) => Err(e),
    }
}

fn do_persist_update(
    hdd_store: &mut HDDStorage,
    ssd_buf: &mut FixedSizeSliceBuf<impl EvictStrategySlice>,
    id: BlockId,
) -> SUResult<WorkerResponse> {
    let response = ssd_buf
        .pop_one(id)
        .map(|update| {
            let mut ranges = Ranges::empty();
            let mut cursor = 0;
            let payload = update
                .data
                .slices
                .into_iter()
                .filter_map(|slice| match slice {
                    crate::storage::SliceOpt::Present(data) => {
                        let range = cursor..cursor + data.len();
                        ranges
                            .0
                            .intersection_with(&range_collections::RangeSet2::from(range.clone()));
                        hdd_store
                            .put_slice(id, cursor, &data)
                            .expect("fail to put slice"); // TODO: handle error
                        cursor += data.len();
                        Some(data)
                    }
                    crate::storage::SliceOpt::Absent(size) => {
                        cursor += size;
                        None
                    }
                })
                .flatten()
                .collect::<Vec<_>>();
            WorkerResponse::PersistUpdate(ranges, payload)
        })
        .unwrap_or_else(|| WorkerResponse::Nak(format!("no update for block {id}")));
    Ok(response)
}

fn do_buffer_update_data(
    ssd_buf: &mut FixedSizeSliceBuf<impl EvictStrategySlice>,
    id: BlockId,
    ranges: Ranges,
    data: Vec<u8>,
) -> SUResult<WorkerResponse> {
    let mut cursor = 0;
    let result = ranges.to_ranges().iter().try_for_each(|range| {
        let update_slice = &data[cursor..cursor + range.len()];
        let result = ssd_buf.push_slice(id, range.start, update_slice);
        cursor += range.len();
        match result {
            Ok(Some(_)) => unreachable!(),
            Ok(None) => Ok(()),
            Err(e) => Err(WorkerResponse::Nak(format!("fail to buffer updates: {e}"))),
        }
    });
    Ok(match result {
        Ok(_) => WorkerResponse::BufferUpdateData,
        Err(r) => r,
    })
}

fn do_update_parity(
    hdd_store: &mut HDDStorage,
    id: BlockId,
    ranges: Ranges,
    data: Vec<u8>,
) -> SUResult<WorkerResponse> {
    let mut cursor = 0;
    let result = ranges.to_ranges().iter().try_for_each(|range| {
        let slice_data = &data[cursor..cursor + range.len()];
        let result = hdd_store.put_slice(id, range.start, slice_data);
        cursor += range.len();
        match result {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(WorkerResponse::Nak(format!("block id {id} not found"))),
            Err(e) => Err(WorkerResponse::Nak(format!("fail to update parity: {e}"))),
        }
    });
    Ok(match result {
        Ok(_) => WorkerResponse::UpdateParity,
        Err(r) => r,
    })
}

fn do_flush_buf(
    ssd_buf: &mut FixedSizeSliceBuf<impl EvictStrategySlice>,
) -> SUResult<WorkerResponse> {
    Ok(ssd_buf
        .cleanup_dev()
        .map(|_| WorkerResponse::FlushBuf)
        .unwrap_or_else(|e| WorkerResponse::Nak(format!("fail to flush buffer: {e}"))))
}

fn do_drop_store(hdd_store: &mut HDDStorage) -> SUResult<WorkerResponse> {
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
        .map(|_| WorkerResponse::DropStore)
        .unwrap_or_else(|e| WorkerResponse::Nak(format!("fail to drop store: {e}")));
    Ok(response)
}

fn do_heartbeat(worker_id: WorkerID) -> SUResult<WorkerResponse> {
    Ok(WorkerResponse::HeartBeat(worker_id))
}

fn do_shutdown(worker_id: WorkerID) -> SUResult<WorkerResponse> {
    Ok(WorkerResponse::Shutdown(worker_id))
}
