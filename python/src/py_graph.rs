use std::sync::{Arc, Mutex};

#[pyclass]
pub(crate) struct PyGraph {
    internal_graph: Arc<Mutex<InternalGraph>>,
}
