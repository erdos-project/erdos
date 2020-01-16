use abomonation::Abomonation;
use serde::Deserialize;

use crate::{
    communication::{SendEndpoint, Serializable},
    dataflow::{Data, Message, Timestamp, WriteStreamError},
};

use super::{StreamId, WriteStreamT};

// TODO: refactor with internal write stream
#[derive(Clone)]
pub struct WriteStream<D: Data> {
    /// StreamId of the stream.
    id: StreamId,
    /// User-defined stream name.
    name: String,
    /// Send to other threads in the same process.
    inter_thread_endpoints: Vec<SendEndpoint<Message<D>>>,
    /// Send to other processes.
    inter_process_endpoints: Vec<SendEndpoint<Message<D>>>,
    /// Current low watermark.
    low_watermark: Timestamp,
}

impl<D: Data> WriteStream<D> {
    pub fn new() -> Self {
        let id = StreamId::new_deterministic();
        WriteStream::new_internal(id, id.to_string())
    }

    pub fn new_from_name(name: &str) -> Self {
        WriteStream::new_internal(StreamId::new_deterministic(), name.to_string())
    }

    pub fn new_from_id(id: StreamId) -> Self {
        WriteStream::new_internal(id, id.to_string())
    }

    fn new_internal(id: StreamId, name: String) -> Self {
        Self {
            id,
            name,
            inter_thread_endpoints: Vec::new(),
            inter_process_endpoints: Vec::new(),
            low_watermark: Timestamp::new(vec![0]),
        }
    }

    pub fn from_endpoints(endpoints: Vec<SendEndpoint<Message<D>>>, id: StreamId) -> Self {
        let mut stream = Self::new_from_id(id);
        for endpoint in endpoints {
            stream.add_endpoint(endpoint);
        }
        stream
    }

    pub fn get_id(&self) -> StreamId {
        self.id
    }

    pub fn get_name(&self) -> &str {
        &self.name[..]
    }

    fn add_endpoint(&mut self, endpoint: SendEndpoint<Message<D>>) {
        match endpoint {
            SendEndpoint::InterThread(_) => self.inter_thread_endpoints.push(endpoint),
            SendEndpoint::InterProcess(_, _) => self.inter_process_endpoints.push(endpoint),
        }
    }

    fn update_watermark(&mut self, msg: &Message<D>) -> Result<(), WriteStreamError> {
        match msg {
            Message::TimestampedData(td) => {
                if td.timestamp < self.low_watermark {
                    return Err(WriteStreamError::TimestampError);
                }
            }
            Message::Watermark(msg_watermark) => {
                if msg_watermark < &self.low_watermark {
                    return Err(WriteStreamError::TimestampError);
                }
                self.low_watermark = msg_watermark.clone();
            }
        }
        Ok(())
    }
}

impl<D: Data> Default for WriteStream<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, D: Data + Deserialize<'a>> WriteStreamT<D> for WriteStream<D> {
    /// Specialized implementation for when the Data does not implement `Abomonation`.
    default fn send(&mut self, msg: Message<D>) -> Result<(), WriteStreamError> {
        self.update_watermark(&msg)?;
        if !self.inter_process_endpoints.is_empty() {
            // Serialize the message because we have endpoints in different processes.
            let serialized_msg = msg.encode().map_err(WriteStreamError::from)?;
            // Copy the serialized message n-1 times.
            for i in 1..self.inter_process_endpoints.len() {
                self.inter_process_endpoints[i]
                    .send_from_bytes(serialized_msg.clone())
                    .map_err(WriteStreamError::from)?;
            }
            self.inter_process_endpoints[0]
                .send_from_bytes(serialized_msg)
                .map_err(WriteStreamError::from)?;
        }

        if self.inter_thread_endpoints.len() > 0 {
            for i in 1..self.inter_thread_endpoints.len() {
                self.inter_thread_endpoints[i]
                    .send(msg.clone())
                    .map_err(WriteStreamError::from)?;
            }
            self.inter_thread_endpoints[0]
                .send(msg)
                .map_err(WriteStreamError::from)?;
        }
        Ok(())
    }
}

impl<'a, D: Data + Deserialize<'a> + Abomonation> WriteStreamT<D> for WriteStream<D> {
    /// Specialized implementation for when the Data implements `Abomonation`.
    fn send(&mut self, msg: Message<D>) -> Result<(), WriteStreamError> {
        self.update_watermark(&msg)?;
        if !self.inter_process_endpoints.is_empty() {
            let serialized_msg = msg.encode().map_err(WriteStreamError::from)?;
            for i in 1..self.inter_process_endpoints.len() {
                self.inter_process_endpoints[i]
                    .send_from_bytes(serialized_msg.clone())
                    .map_err(WriteStreamError::from)?;
            }
            self.inter_process_endpoints[0]
                .send_from_bytes(serialized_msg)
                .map_err(WriteStreamError::from)?;
        }

        if !self.inter_thread_endpoints.is_empty() {
            for i in 1..self.inter_thread_endpoints.len() {
                self.inter_thread_endpoints[i]
                    .send(msg.clone())
                    .map_err(WriteStreamError::from)?;
            }
            self.inter_thread_endpoints[0]
                .send(msg)
                .map_err(WriteStreamError::from)?;
        }

        Ok(())
    }
}
