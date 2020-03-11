use pyo3::{exceptions, prelude::*};

use crate::dataflow::StatefulReadStream;

mod py_extract_stream;
mod py_ingest_stream;
mod py_loop_stream;
mod py_read_stream;
mod py_write_stream;

pub use py_extract_stream::PyExtractStream;
pub use py_ingest_stream::PyIngestStream;
pub use py_loop_stream::PyLoopStream;
pub use py_read_stream::PyReadStream;
pub use py_write_stream::PyWriteStream;

// TODO: find a nicer implementation
// TODO: find a way to add write streams without introducing n^2 different if statements
pub fn add_watermark_callback(
    read_streams: Vec<&PyReadStream>,
    callback: PyObject,
) -> PyResult<()> {
    if read_streams.len() == 1 {
        read_streams[0].add_watermark_callback(callback);
        return Ok(());
    }

    let stateful_read_streams: Vec<StatefulReadStream<Vec<u8>, ()>> = read_streams
        .iter()
        .map(|py_rs| py_rs.read_stream.add_state(()))
        .collect();

    let two_read = stateful_read_streams[0].add_read_stream(&stateful_read_streams[1]);

    if stateful_read_streams.len() == 2 {
        two_read
            .borrow_mut()
            .add_watermark_callback(move |timestamp, _s0, _s1| {
                let gil = Python::acquire_gil();
                let py = gil.python();
                match callback.call1(py, (timestamp.time.clone(), timestamp.is_top())) {
                    Ok(_) => (),
                    Err(e) => e.print(py),
                };
            });
        return Ok(());
    }

    let three_read = two_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[2]);

    if stateful_read_streams.len() == 3 {
        three_read
            .borrow_mut()
            .add_watermark_callback(move |timestamp, _s0, _s1, _s2| {
                let gil = Python::acquire_gil();
                let py = gil.python();
                match callback.call1(py, (timestamp.time.clone(), timestamp.is_top())) {
                    Ok(_) => (),
                    Err(e) => e.print(py),
                };
            });
        return Ok(());
    }

    let four_read = three_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[3]);

    if stateful_read_streams.len() == 4 {
        four_read
            .borrow_mut()
            .add_watermark_callback(move |timestamp, _s0, _s1, _s2, _s3| {
                let gil = Python::acquire_gil();
                let py = gil.python();
                match callback.call1(py, (timestamp.time.clone(), timestamp.is_top())) {
                    Ok(_) => (),
                    Err(e) => e.print(py),
                };
            });
        return Ok(());
    }

    let five_read = four_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[4]);

    if stateful_read_streams.len() == 5 {
        five_read
            .borrow_mut()
            .add_watermark_callback(move |timestamp, _s0, _s1, _s2, _s3, _s4| {
                let gil = Python::acquire_gil();
                let py = gil.python();
                match callback.call1(py, (timestamp.time.clone(), timestamp.is_top())) {
                    Ok(_) => (),
                    Err(e) => e.print(py),
                };
            });
        return Ok(());
    }

    let six_read = five_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[5]);

    if stateful_read_streams.len() == 6 {
        six_read.borrow_mut().add_watermark_callback(
            move |timestamp, _s0, _s1, _s2, _s3, _s4, _s5| {
                let gil = Python::acquire_gil();
                let py = gil.python();
                match callback.call1(py, (timestamp.time.clone(), timestamp.is_top())) {
                    Ok(_) => (),
                    Err(e) => e.print(py),
                };
            },
        );
        return Ok(());
    }

    let seven_read = six_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[6]);

    if stateful_read_streams.len() == 7 {
        seven_read.borrow_mut().add_watermark_callback(
            move |timestamp, _s0, _s1, _s2, _s3, _s4, _s5, _s6| {
                let gil = Python::acquire_gil();
                let py = gil.python();
                match callback.call1(py, (timestamp.time.clone(), timestamp.is_top())) {
                    Ok(_) => (),
                    Err(e) => e.print(py),
                };
            },
        );
        return Ok(());
    }

    let eight_read = seven_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[7]);

    if stateful_read_streams.len() == 8 {
        eight_read.borrow_mut().add_watermark_callback(
            move |timestamp, _s0, _s1, _s2, _s3, _s4, _s5, _s6, _s7| {
                let gil = Python::acquire_gil();
                let py = gil.python();
                match callback.call1(py, (timestamp.time.clone(), timestamp.is_top())) {
                    Ok(_) => (),
                    Err(e) => e.print(py),
                };
            },
        );
        return Ok(());
    }

    Err(PyErr::new::<exceptions::TypeError, _>(
        "Unable to create watermark callback across more than eight read streams",
    ))
}
