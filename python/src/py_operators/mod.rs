use std::sync::Arc;

use erdos::dataflow::operator::OperatorConfig;
use pyo3::{prelude::*, types::*};

// Private submodules
mod py_one_in_one_out;
mod py_one_in_two_out;
mod py_sink;
mod py_source;
mod py_two_in_one_out;

// Crate-level exports
pub(crate) use py_one_in_one_out::*;
pub(crate) use py_one_in_two_out::*;
pub(crate) use py_sink::*;
pub(crate) use py_source::*;
pub(crate) use py_two_in_one_out::*;

fn construct_operator(
    py_operator_type: Arc<PyObject>,
    py_operator_args: Arc<PyObject>,
    py_operator_kwargs: Arc<PyObject>,
    py_operator_config: Arc<PyObject>,
    config: OperatorConfig,
) -> Arc<PyObject> {
    // TODO (Sukrit): The function should return a Result object instead of echoing errors to
    // standard output.
    Python::with_gil(|py| -> Arc<PyObject> {
        let locals = PyDict::new(py);
        if let Some(e) = locals
            .set_item("Operator", py_operator_type.clone_ref(py))
            .err()
        {
            e.print(py)
        }
        if let Some(e) = locals.set_item("op_id", format!("{}", config.id)).err() {
            e.print(py)
        }
        if let Some(e) = locals
            .set_item("args", py_operator_args.clone_ref(py))
            .err()
        {
            e.print(py)
        }
        if let Some(e) = locals
            .set_item("kwargs", py_operator_kwargs.clone_ref(py))
            .err()
        {
            e.print(py)
        }
        if let Some(e) = locals
            .set_item("config", py_operator_config.clone_ref(py))
            .err()
        {
            e.print(py)
        }
        if let Some(e) = locals.set_item("op_name", config.get_name()).err() {
            e.print(py)
        }

        // Initialize the operator.
        let init_result = py.run(
            r#"
import uuid, erdos

# Create the operator.
operator = Operator.__new__(Operator)
operator._id = uuid.UUID(op_id)
operator._config = config
operator._trace_event_logger = erdos.utils.setup_trace_logging(
    "{}-profile".format(op_name), 
    config.profile_file_name,
)
operator.__init__(*args, **kwargs)
            "#,
            None,
            Some(locals),
        );
        if let Err(e) = init_result {
            e.print(py);
        }

        // Retrieve the constructed operator.
        Arc::new(
            py.eval("operator", None, Some(locals))
                .unwrap()
                .to_object(py),
        )
    })
}
