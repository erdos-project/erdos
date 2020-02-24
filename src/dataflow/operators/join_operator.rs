use std::marker::PhantomData;

use crate::{
    add_watermark_callback,
    dataflow::{Data, OperatorConfig, OperatorT, ReadStream, Timestamp, WriteStream},
    OperatorId,
};

#[derive(Clone, Default)]
struct WindowState<D> {
    pub msgs: Vec<D>,
}

impl<D> WindowState<D> {
    pub fn new() -> Self {
        Self { msgs: Vec::new() }
    }
}

pub struct JoinOperator<D1: Data, D2: Data, D3: Data> {
    name: String,
    id: OperatorId,
    phantom: PhantomData<(D1, D2, D3)>,
}

impl<D1: Data, D2: Data, D3: Data> OperatorT for JoinOperator<D1, D2, D3> {
    fn get_id(&self) -> OperatorId {
        self.id
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }
}

impl<D1: Data, D2: Data, D3: Data> JoinOperator<D1, D2, D3> {
    #[allow(dead_code)]
    pub fn new(
        config: OperatorConfig<()>,
        left_stream: ReadStream<D1>,
        right_stream: ReadStream<D2>,
        write_stream: WriteStream<D3>,
    ) -> Self {
        let stateful_ls = left_stream.add_state(WindowState::<D1>::new());
        stateful_ls.add_callback(JoinOperator::<D1, D2, D3>::on_left_data);
        let stateful_rs = right_stream.add_state(WindowState::<D2>::new());
        stateful_rs.add_callback(JoinOperator::<D1, D2, D3>::on_right_data);
        add_watermark_callback!(
            (stateful_ls, stateful_rs),
            (write_stream),
            (JoinOperator::<D1, D2, D3>::join)
        );

        Self {
            name: config.name.unwrap(),
            id: config.id,
            phantom: PhantomData,
        }
    }

    #[allow(dead_code)]
    pub fn connect(
        _left_stream: &ReadStream<D1>,
        _right_stream: &ReadStream<D2>,
    ) -> WriteStream<D3> {
        WriteStream::new()
    }

    #[allow(dead_code)]
    fn on_left_data(_t: Timestamp, msg: D1, state: &mut WindowState<D1>) {
        state.msgs.push(msg);
    }

    #[allow(dead_code)]
    fn on_right_data(_t: Timestamp, msg: D2, state: &mut WindowState<D2>) {
        state.msgs.push(msg);
    }

    fn join(
        _t: &Timestamp,
        left_state: &WindowState<D1>,
        right_state: &WindowState<D2>,
        write_stream: &mut WriteStream<D3>,
    ) {
        // TODO(ionel): Access the selector functions.
    }

    pub fn run(&self) {}
}
