use pyo3::{exceptions, prelude::*};

use crate::dataflow::StatefulReadStream;

// Private submodules
mod py_extract_stream;
mod py_ingest_stream;
mod py_loop_stream;
mod py_read_stream;
mod py_write_stream;

// Public exports
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
    // TODO FIX THIS FOR PYTHON
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

    let nine_read = eight_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[8]);

    if stateful_read_streams.len() == 9 {
        nine_read.borrow_mut().add_watermark_callback(
            move |timestamp, _s0, _s1, _s2, _s3, _s4, _s5, _s6, _s7, _s8| {
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

    let ten_read = nine_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[9]);

    if stateful_read_streams.len() == 10 {
        ten_read.borrow_mut().add_watermark_callback(
            move |timestamp, _s0, _s1, _s2, _s3, _s4, _s5, _s6, _s7, _s8, _s9| {
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

    let eleven_read = ten_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[10]);

    if stateful_read_streams.len() == 11 {
        eleven_read.borrow_mut().add_watermark_callback(
            move |timestamp, _s0, _s1, _s2, _s3, _s4, _s5, _s6, _s7, _s8, _s9, _s10| {
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

    let twelve_read = eleven_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[11]);

    if stateful_read_streams.len() == 12 {
        twelve_read.borrow_mut().add_watermark_callback(
            move |timestamp, _s0, _s1, _s2, _s3, _s4, _s5, _s6, _s7, _s8, _s9, _s10, _s11| {
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

    let thirteen_read = twelve_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[12]);

    if stateful_read_streams.len() == 13 {
        thirteen_read.borrow_mut().add_watermark_callback(
            move |timestamp, _s0, _s1, _s2, _s3, _s4, _s5, _s6, _s7, _s8, _s9, _s10, _s11, _s12| {
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

    let fourteen_read = thirteen_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[13]);

    if stateful_read_streams.len() == 14 {
        fourteen_read.borrow_mut().add_watermark_callback(
            move |timestamp,
                  _s0,
                  _s1,
                  _s2,
                  _s3,
                  _s4,
                  _s5,
                  _s6,
                  _s7,
                  _s8,
                  _s9,
                  _s10,
                  _s11,
                  _s12,
                  _s13| {
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

    let fifteen_read = fourteen_read
        .borrow_mut()
        .add_read_stream(&stateful_read_streams[14]);

    if stateful_read_streams.len() == 15 {
        fifteen_read.borrow_mut().add_watermark_callback(
            move |timestamp,
                  _s0,
                  _s1,
                  _s2,
                  _s3,
                  _s4,
                  _s5,
                  _s6,
                  _s7,
                  _s8,
                  _s9,
                  _s10,
                  _s11,
                  _s12,
                  _s13,
                  _s14| {
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
        "Unable to create watermark callback across more than fifteen read streams",
    ))
}
