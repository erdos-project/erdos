use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use serde::Deserialize;

use crate::{
    dataflow::{graph::default_graph, Data, Message},
    node::NodeId,
    scheduler::channel_manager::ChannelManager,
};

use super::{
    errors::{ReadError, TryReadError},
    InternalReadStream, ReadStream, StreamId,
};

/// Enables reading data from streams in the driver of a running ERDOS
/// application.
pub struct ExtractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    id: StreamId,
    node_id: NodeId,
    read_stream_option: Option<ReadStream<D>>,
    // Used to circumvent requiring Send to transfer ReadStream across threads
    channel_manager_option: Arc<Mutex<Option<Arc<Mutex<ChannelManager>>>>>,
}

impl<D> ExtractStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub fn new(node_id: NodeId, read_stream: &ReadStream<D>) -> Self {
        let id = read_stream.get_id();

        let extract_stream = Self {
            id,
            node_id,
            read_stream_option: None,
            channel_manager_option: Arc::new(Mutex::new(None)),
        };
        let channel_manager_option_copy = Arc::clone(&extract_stream.channel_manager_option);

        // Sets up self.read_stream_option using channel_manager
        let setup_hook = move |channel_manager: Arc<Mutex<ChannelManager>>| {
            channel_manager_option_copy
                .lock()
                .unwrap()
                .replace(channel_manager);
        };

        default_graph::add_extract_stream(&extract_stream, setup_hook);
        extract_stream
    }

    pub fn get_id(&self) -> StreamId {
        self.id
    }

    pub fn get_node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns true if a top watermark message was sent or the ExtractStream was unable to set up.
    pub fn is_closed(&self) -> bool {
        self.read_stream_option
            .as_ref()
            .map(ReadStream::is_closed)
            .unwrap_or(true)
    }

    /// Tries to read a message from a channel.
    ///
    /// Returns an immutable reference, or `None` if no messages are
    /// available at the moment (i.e., non-blocking read).
    pub fn try_read(&mut self) -> Result<Message<D>, TryReadError> {
        if let Some(read_stream) = &self.read_stream_option {
            read_stream.try_read()
        } else {
            // Try to setup read stream
            if let Some(channel_manager) = &*self.channel_manager_option.lock().unwrap() {
                match channel_manager.lock().unwrap().take_recv_endpoint(self.id) {
                    Ok(recv_endpoint) => {
                        let read_stream = ReadStream::from(InternalReadStream::from_endpoint(
                            recv_endpoint,
                            self.id,
                        ));
                        let result = read_stream.try_read();
                        self.read_stream_option.replace(read_stream);
                        return result;
                    }
                    Err(msg) => eprintln!(
                        "ExtractStream {}: error getting endpoint from channel manager \"{}\"",
                        self.id, msg
                    ),
                }
            }
            Err(TryReadError::Disconnected)
        }
    }

    /// Blocking read. Returns `None` if the stream doesn't have a receive endpoint.
    pub fn read(&mut self) -> Result<Message<D>, ReadError> {
        loop {
            let result = self.try_read();
            if self.read_stream_option.is_some() {
                break match result {
                    Ok(msg) => Ok(msg),
                    Err(TryReadError::Disconnected) => Err(ReadError::Disconnected),
                    Err(TryReadError::Empty) => self.read_stream_option.as_ref().unwrap().read(),
                    Err(TryReadError::SerializationError) => Err(ReadError::SerializationError),
                    Err(TryReadError::Closed) => Err(ReadError::Closed),
                };
            } else {
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

// Needed to avoid deadlock in Python
unsafe impl<D> Send for ExtractStream<D> where for<'a> D: Data + Deserialize<'a> {}
