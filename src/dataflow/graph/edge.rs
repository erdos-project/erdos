use std::marker::PhantomData;

use serde::Deserialize;

use crate::{
    dataflow::{stream::StreamId, Data},
    scheduler::channel_manager::{StreamEndpoints, StreamEndpointsT},
};

use super::Vertex;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Channel {
    InterThread(ChannelMetadata),
    InterNode(ChannelMetadata),
    Unscheduled(ChannelMetadata),
}

/// Stores metadata about a data-flow channel.
///
/// A data-flow channel is an edge in the data-flow graph.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChannelMetadata {
    pub stream_id: StreamId,
    pub source: Vertex,
    pub sink: Vertex,
}

impl ChannelMetadata {
    pub fn new(stream_id: StreamId, source: Vertex, sink: Vertex) -> Self {
        Self {
            stream_id,
            source,
            sink,
        }
    }
}

#[derive(Clone)]
pub struct TypedStreamMetadata<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    id: StreamId,
    source: Vertex,
    channels: Vec<Channel>,
    phantom: PhantomData<D>,
}

impl<D> TypedStreamMetadata<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    pub fn new(id: StreamId, source: Vertex) -> Self {
        Self {
            id,
            source,
            channels: Vec::new(),
            phantom: PhantomData,
        }
    }
}

pub trait StreamMetadataT: Send {
    fn id(&self) -> StreamId;
    fn source(&self) -> Vertex;
    fn box_clone(&self) -> Box<dyn StreamMetadataT>;
    fn to_stream_endpoints_t(&self) -> Box<dyn StreamEndpointsT>;
    fn add_channel(&mut self, channel: Channel);
    fn get_channels(&self) -> Vec<Channel>;
    fn set_channels(&mut self, channels: Vec<Channel>);
}

impl<D> StreamMetadataT for TypedStreamMetadata<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    fn id(&self) -> StreamId {
        self.id
    }

    fn source(&self) -> Vertex {
        self.source.clone()
    }

    fn box_clone(&self) -> Box<dyn StreamMetadataT> {
        Box::new(self.clone())
    }

    fn to_stream_endpoints_t(&self) -> Box<dyn StreamEndpointsT> {
        Box::new(StreamEndpoints::<D>::new(self.id))
    }

    fn add_channel(&mut self, channel: Channel) {
        self.channels.push(channel);
    }

    fn get_channels(&self) -> Vec<Channel> {
        self.channels.clone()
    }

    fn set_channels(&mut self, channels: Vec<Channel>) {
        self.channels = channels;
    }
}

pub struct StreamMetadata {
    stream_metadata_t: Box<dyn StreamMetadataT>,
}

impl StreamMetadata {
    pub fn new<D>(id: StreamId, source: Vertex) -> Self
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        Self {
            stream_metadata_t: Box::new(TypedStreamMetadata::<D>::new(id, source)),
        }
    }

    pub fn id(&self) -> StreamId {
        self.stream_metadata_t.id()
    }

    pub fn source(&self) -> Vertex {
        self.stream_metadata_t.source()
    }

    pub fn to_stream_endpoints_t(&self) -> Box<dyn StreamEndpointsT> {
        self.stream_metadata_t.to_stream_endpoints_t()
    }

    pub fn add_channel(&mut self, channel: Channel) {
        self.stream_metadata_t.add_channel(channel);
    }

    pub fn get_channels(&self) -> Vec<Channel> {
        self.stream_metadata_t.get_channels()
    }

    pub fn set_channels(&mut self, channels: Vec<Channel>) {
        self.stream_metadata_t.set_channels(channels)
    }
}

impl Clone for StreamMetadata {
    fn clone(&self) -> Self {
        Self {
            stream_metadata_t: self.stream_metadata_t.box_clone(),
        }
    }
}
