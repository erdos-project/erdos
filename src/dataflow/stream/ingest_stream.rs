use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use serde::Deserialize;

use crate::{
    dataflow::{graph::default_graph, Data, Message, WriteStreamError},
    node::NodeId,
    scheduler::channel_manager::ChannelManager,
};

use super::{StreamId, WriteStream, WriteStreamT};

pub struct IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    id: StreamId,
    name: String,
    node_id: NodeId,
    write_stream_option: Arc<Mutex<Option<WriteStream<D>>>>,
}

impl<D> IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub fn new(node_id: NodeId) -> Self {
        let id = StreamId::new_deterministic();
        IngestStream::new_internal(node_id, id, id.to_string())
    }

    pub fn new_with_name(node_id: NodeId, name: &str) -> Self {
        let id = StreamId::new_deterministic();
        IngestStream::new_internal(node_id, id, name.to_string())
    }

    fn new_internal(node_id: NodeId, id: StreamId, name: String) -> Self {
        let ingest_stream = Self {
            id,
            name,
            node_id,
            write_stream_option: Arc::new(Mutex::new(None)),
        };
        let write_stream_option_copy = Arc::clone(&ingest_stream.write_stream_option);

        // Sets up self.write_stream_option using channel_manager
        let setup_hook = move |channel_manager: Arc<Mutex<ChannelManager>>| match channel_manager
            .lock()
            .unwrap()
            .get_send_endpoints(id)
        {
            Ok(send_endpoints) => {
                let write_stream = WriteStream::from_endpoints(send_endpoints, id);
                write_stream_option_copy
                    .lock()
                    .unwrap()
                    .replace(write_stream);
            }
            Err(msg) => eprintln!("Unable to set up IngestStream {}: {}", id, msg),
        };

        default_graph::add_ingest_stream(&ingest_stream, setup_hook);
        ingest_stream
    }

    pub fn get_id(&self) -> StreamId {
        self.id
    }

    pub fn get_name(&self) -> &str {
        &self.name[..]
    }

    pub fn get_node_id(&self) -> NodeId {
        self.node_id
    }
}

impl<D> WriteStreamT<D> for IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// Blocks until write stream is available
    fn send(&mut self, msg: Message<D>) -> Result<(), WriteStreamError> {
        loop {
            {
                if let Some(write_stream) = self.write_stream_option.lock().unwrap().as_mut() {
                    let res = write_stream.send(msg);
                    return res;
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}
