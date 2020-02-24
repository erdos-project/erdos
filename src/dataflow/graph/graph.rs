use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;

use serde::Deserialize;

use crate::{
    dataflow::{
        stream::{ExtractStream, IngestStream, LoopStream, StreamId, WriteStream},
        Data,
    },
    node::NodeId,
    OperatorId,
};

use super::{
    Channel, ChannelMetadata, DriverMetadata, OperatorMetadata, OperatorRunner, StreamMetadata,
    StreamSetupHook, Vertex,
};

/// Represents a data-flow computation.
///
/// Operators correspond to vertices, and channels correspond to edges.
/// Streams are collections of related channels. Writing on one stream
/// broadcasts the message on all channels belonging to that stream.
#[derive(Clone)]
pub struct Graph {
    /// Mapping between operator ids and operator metadata.
    operators: HashMap<OperatorId, OperatorMetadata>,
    /// Mapping between node ids and driver metadata.
    drivers: HashMap<NodeId, DriverMetadata>,
    /// Mapping between stream ids and stream metadata.
    streams: HashMap<StreamId, StreamMetadata>,
    /// ID mappings for streams aliasing other streams, e.g. LoopStreams
    stream_aliases: HashMap<StreamId, StreamId>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            operators: HashMap::new(),
            drivers: HashMap::new(),
            streams: HashMap::new(),
            stream_aliases: HashMap::new(),
        }
    }

    pub fn add_operator<F: OperatorRunner>(
        &mut self,
        id: OperatorId,
        name: Option<String>,
        node_id: NodeId,
        read_stream_ids: Vec<StreamId>,
        write_stream_ids: Vec<StreamId>,
        runner: F,
    ) {
        let read_stream_ids: Vec<StreamId> = read_stream_ids
            .into_iter()
            .map(|id| self.resolve_stream_id(id))
            .collect();
        let write_stream_ids: Vec<StreamId> = write_stream_ids
            .into_iter()
            .map(|id| self.resolve_stream_id(id))
            .collect();

        // Add channels for this operator's read streams.
        for read_stream_id in &read_stream_ids {
            if let Some(stream) = self.streams.get_mut(read_stream_id) {
                stream.add_channel(Channel::Unscheduled(ChannelMetadata::new(
                    stream.get_id(),
                    stream.get_source(),
                    Vertex::Operator(id),
                )));
            }
        }

        self.operators.insert(
            id,
            OperatorMetadata::new(id, name, node_id, read_stream_ids, write_stream_ids, runner),
        );
    }

    pub fn add_operator_stream<D>(&mut self, operator_id: OperatorId, write_stream: &WriteStream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let stream_id = write_stream.get_id();
        let mut stream_metadata =
            StreamMetadata::new::<D>(stream_id, Vertex::Operator(operator_id));
        self.add_channels(&mut stream_metadata);
        self.streams.insert(stream_id, stream_metadata);
    }

    pub fn add_ingest_stream<D, F: StreamSetupHook>(
        &mut self,
        ingest_stream: &IngestStream<D>,
        setup_hook: F,
    ) where
        for<'a> D: Data + Deserialize<'a>,
    {
        let stream_id = ingest_stream.get_id();
        let node_id = ingest_stream.get_node_id();
        // Add stream to driver
        let driver = self
            .drivers
            .entry(ingest_stream.get_node_id())
            .or_insert_with(|| DriverMetadata::new(node_id));
        driver.add_ingest_stream(stream_id, setup_hook);
        // Add stream to graph
        let mut stream_metadata = StreamMetadata::new::<D>(stream_id, Vertex::Driver(node_id));
        self.add_channels(&mut stream_metadata);
        self.streams.insert(stream_id, stream_metadata);
    }

    pub fn add_extract_stream<D, F: StreamSetupHook>(
        &mut self,
        extract_stream: &ExtractStream<D>,
        setup_hook: F,
    ) where
        for<'a> D: Data + Deserialize<'a>,
    {
        let stream_id = extract_stream.get_id();
        let node_id = extract_stream.get_node_id();
        // Add stream to driver
        let driver = self
            .drivers
            .entry(extract_stream.get_node_id())
            .or_insert_with(|| DriverMetadata::new(node_id));
        driver.add_extract_stream(stream_id, setup_hook);
        // Add channel to stream
        if let Some(stream_metadata) = self.streams.get_mut(&stream_id) {
            let channel = Channel::Unscheduled(ChannelMetadata::new(
                stream_id,
                stream_metadata.get_source(),
                Vertex::Driver(node_id),
            ));
            stream_metadata.add_channel(channel);
        } else {
            eprintln!("Graph: unable to add extract stream {}", stream_id);
        }
    }

    pub fn add_loop_stream<D>(&mut self, loop_stream: &LoopStream<D>)
    where
        for<'a> D: Data + Deserialize<'a>,
    {
        let write_stream = WriteStream::<D>::new_from_id(loop_stream.get_id());
        // TODO: clean up this hack
        self.add_operator_stream(OperatorId::nil(), &write_stream);
    }

    pub fn resolve_stream_id(&self, stream_id: StreamId) -> StreamId {
        // TODO: maybe call recursively to look up
        match self.stream_aliases.get(&stream_id) {
            Some(&fixed_id) => fixed_id,
            None => stream_id,
        }
    }

    pub fn add_stream_alias(&mut self, from_id: StreamId, to_id: StreamId) -> Result<(), String> {
        if !self.streams.contains_key(&to_id) {
            return Err(format!(
                "Dataflow graph does not contain stream with ID {}",
                to_id
            ));
        }

        self.stream_aliases.insert(from_id, to_id);

        // Merge stream infos
        if let Some(from_stream) = self.streams.remove(&from_id) {
            match self.streams.get_mut(&to_id) {
                Some(to_stream) => {
                    for channel in from_stream.get_channels() {
                        let mut channel_metadata = match channel {
                            Channel::InterNode(cm) => cm,
                            Channel::InterThread(cm) => cm,
                            Channel::Unscheduled(cm) => cm,
                        };
                        channel_metadata.stream_id = to_id;
                        channel_metadata.source = to_stream.get_source();
                        to_stream.add_channel(Channel::Unscheduled(channel_metadata));
                    }
                }
                None => unreachable!(),
            }
        }
        Ok(())
    }

    /// Adds channels to the StreamMetadata based on the graph
    fn add_channels(&self, stream_metadata: &mut StreamMetadata) {
        let stream_id = stream_metadata.get_id();
        let source = stream_metadata.get_source();
        // Add channels to operators
        for operator in self.operators.values() {
            for _ in operator
                .read_stream_ids
                .iter()
                .filter(|&&id| stream_id == id)
            {
                stream_metadata.add_channel(Channel::Unscheduled(ChannelMetadata::new(
                    stream_id,
                    source.clone(),
                    Vertex::Operator(operator.id),
                )));
            }
        }
        // Add channels to drivers
        for driver in self.drivers.values() {
            for _ in driver
                .extract_stream_ids
                .iter()
                .filter(|&&id| stream_id == id)
            {
                stream_metadata.add_channel(Channel::Unscheduled(ChannelMetadata::new(
                    stream_id,
                    source.clone(),
                    Vertex::Driver(driver.id),
                )));
            }
        }
    }

    pub fn get_operator(&self, operator_id: OperatorId) -> Option<OperatorMetadata> {
        self.operators.get(&operator_id).cloned()
    }

    pub fn get_operators(&self) -> Vec<OperatorMetadata> {
        self.operators.values().cloned().collect()
    }

    pub fn get_driver(&self, node_id: NodeId) -> Option<DriverMetadata> {
        self.drivers.get(&node_id).cloned()
    }

    pub fn get_stream(&self, stream_id: StreamId) -> Option<StreamMetadata> {
        self.streams.get(&stream_id).cloned()
    }

    pub fn get_streams(&self) -> Vec<StreamMetadata> {
        self.streams.values().cloned().collect()
    }

    pub fn get_streams_ref_mut(&mut self) -> Vec<&mut StreamMetadata> {
        self.streams.values_mut().collect()
    }
    pub fn get_vertices_on(&self, node_id: NodeId) -> Vec<Vertex> {
        let mut result = Vec::new();
        result.extend(
            self.operators
                .values()
                .filter(|op| op.node_id == node_id)
                .map(|op| Vertex::Operator(op.id)),
        );
        result.extend(
            self.drivers
                .values()
                .filter(|d| d.id == node_id)
                .map(|d| Vertex::Driver(d.id)),
        );
        result
    }

    /// Exports the dataflow graph as a DOT file.
    pub fn to_dot(&self, filename: &str) -> std::io::Result<()> {
        let mut file = File::create(filename)?;
        writeln!(file, "digraph erdos_dataflow {{")?;

        // Drivers
        writeln!(file, "   // Declare driver")?;
        for driver_metadata in self.drivers.values() {
            writeln!(
                file,
                "   \"{node_id}\" [label=\"Driver ({node_id})\"];",
                node_id = driver_metadata.id
            )?;
        }

        // Operators
        writeln!(file, "   // Declare operators")?;
        for operator in self.operators.values() {
            let op_name = match &operator.name {
                Some(name) => name.clone(),
                None => format!("{}", operator.id),
            };
            writeln!(
                file,
                "   \"{op_id}\" [label=\"{op_name}\\n(Node {node_id})\"];",
                op_name = op_name,
                op_id = operator.id,
                node_id = operator.node_id
            )?;
        }

        // Channels
        writeln!(file, "   // Declare channels")?;
        for stream in self.streams.values() {
            let from = match stream.get_source() {
                Vertex::Driver(node_id) => format!("{}", node_id),
                Vertex::Operator(op_id) => format!("{}", op_id),
            };
            for channel in stream.get_channels() {
                let channel_metadata = match channel {
                    Channel::InterNode(x) | Channel::InterThread(x) | Channel::Unscheduled(x) => x,
                };
                let to = match channel_metadata.sink {
                    Vertex::Driver(node_id) => format!("{}", node_id),
                    Vertex::Operator(op_id) => format!("{}", op_id),
                };
                writeln!(
                    file,
                    "   \"{from}\" -> \"{to}\" [label=\"{stream_id}\"];",
                    from = from,
                    to = to,
                    stream_id = stream.get_id()
                )?;
            }
        }

        writeln!(file, "}}")?;
        file.flush()?;
        Ok(())
    }
}
