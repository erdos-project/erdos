use crate::dataflow::{
    stream::{WriteStream, WriteStreamT}, operator::Source,
    Data, Message 
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

/// Subscribes to ROS topic and outputs incoming messages to erdos stream using the
/// provided message conversion function.

#[derive(Clone)]
pub struct FromRosOperator<T: rosrust::Message, U>
where
    U: Data + for<'a> Deserialize<'a>,
{
    topic: String,
    to_erdos_msg: Arc<dyn Fn(&T) -> Message<U> + Send + Sync>,
}

impl<T: rosrust::Message, U> FromRosOperator<T, U>
where
    U: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(topic: &str, to_erdos_msg: F) -> Self 
    where
        F: 'static + Fn(&T) -> Message<U> + Send + Sync,
    {
        rosrust::init("subscriber");
        Self {
            topic: topic.to_string(),
            to_erdos_msg: Arc::new(to_erdos_msg),
        }
    }
}

impl<T: rosrust::Message, U> Source<(), U> for FromRosOperator<T, U> 
where
    U: Data + for<'a> Deserialize<'a>, 
{
    fn run(&mut self, write_stream: &mut WriteStream<U>) {
        let to_erdos_msg = self.to_erdos_msg.clone();
        let w_s = Arc::new(Mutex::new(write_stream.clone()));

        let _subscriber_raii = rosrust::subscribe(self.topic.as_str(), 2, move |v: T| {
            let converted = (to_erdos_msg)(&v);
            slog::info!(
                crate::TERMINAL_LOGGER,
                "FromRosOperator: Received and Converted {:?}",
                converted,
            );
            w_s.lock().unwrap().send(converted).unwrap();
        }).unwrap();

        rosrust::spin();
    }
}
