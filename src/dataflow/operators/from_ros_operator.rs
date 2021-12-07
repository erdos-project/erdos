use crate::dataflow::{
    stream::WriteStream, operator::Source,
    Data,
};
use serde::Deserialize;

pub struct FromRosOperator {}

impl FromRosOperator {
    pub fn new() -> Self {
        rosrust::init("subscriber");
        Self {}
    }
}

impl Source<(), String> for FromRosOperator {
    fn run(&mut self, write_stream: &mut WriteStream<String>) {
        let _subscriber_raii = rosrust::subscribe("chatter", 2, |v: rosrust::RawMessage| {
            rosrust::ros_info!("Got: {:?}", v.0);
        }).unwrap();

        rosrust::spin();
    }
}
