use crate::{
    dataflow::{Operator, OperatorConfig, WriteStream},
    OperatorId,
};

#[allow(dead_code)]
pub struct SourceOperator {
    name: String,
    id: OperatorId,
    write_stream: WriteStream<usize>,
}

impl Operator for SourceOperator {}

impl SourceOperator {
    #[allow(dead_code)]
    pub fn new(config: OperatorConfig<()>, write_stream: WriteStream<usize>) -> Self {
        Self {
            name: config.name.unwrap(),
            id: config.id,
            write_stream: write_stream,
        }
    }

    #[allow(dead_code)]
    pub fn connect() -> WriteStream<usize> {
        WriteStream::new()
    }

    pub fn run(&self) {}
}
