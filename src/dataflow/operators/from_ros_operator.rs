use crate::dataflow::{
    stream::WriteStream, operator::Source,
    Data,
};
use serde::Deserialize;
use std::sync::Arc;

pub struct FromRosOperator<T: rosrust::Message, U>
where
    U: Data + for<'a> Deserialize<'a>,
{
    topic: String,
    to_erdos_msg: Arc<dyn Fn(&T) -> U + Send + Sync>,
}

impl<T: rosrust::Message, U: Data> FromRosOperator<T, U>
where
    U: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(topic: &str, to_erdos_msg: F) -> Self 
    where
        F: 'static + Fn(&T) -> U + Send + Sync,
    {
        rosrust::init("subscriber");
        Self {
            topic: topic.to_string(),
            to_erdos_msg: Arc::new(to_erdos_msg),
        }
    }
}

impl<T: rosrust::Message, U: Data> Source<(), U> for FromRosOperator<T, U> 
where
    U: Data + for<'a> Deserialize<'a>,
{
    fn run(&mut self, write_stream: &mut WriteStream<U>) {
        let _subscriber_raii = rosrust::subscribe(self.topic.as_str(), 2, |v: T| {
            rosrust::ros_info!("Converted from Ros: {:?}", (self.to_erdos_msg)(&v));
        }).unwrap();

        rosrust::spin();
    }
}
