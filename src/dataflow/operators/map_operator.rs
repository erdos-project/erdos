use crate::{
    dataflow::{Data, OperatorConfig, OperatorT, ReadStream, Timestamp, WriteStream},
    OperatorId,
};

pub struct MapOperator<D1: Data, D2: Data> {
    name: String,
    id: OperatorId,
    _input_stream: ReadStream<D1>,
    _output_stream: WriteStream<D2>,
}

impl<D1: Data, D2: Data> OperatorT for MapOperator<D1, D2> {
    fn get_id(&self) -> OperatorId {
        self.id
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }
}

impl<D1: Data, D2: Data> MapOperator<D1, D2> {
    #[allow(dead_code)]
    pub fn new(
        config: OperatorConfig<()>,
        input_stream: ReadStream<D1>,
        output_stream: WriteStream<D2>,
    ) -> Self {
        input_stream.add_callback(MapOperator::<D1, D2>::on_data);
        Self {
            name: config.name,
            id: config.id,
            _input_stream: input_stream,
            _output_stream: output_stream,
        }
    }

    #[allow(dead_code)]
    pub fn connect(_input_stream: &ReadStream<D1>) -> WriteStream<D2> {
        WriteStream::new()
    }

    #[allow(dead_code)]
    fn on_data(_t: Timestamp, _msg: D1) {
        println!("Received data");
    }

    pub fn run(&self) {}
}
