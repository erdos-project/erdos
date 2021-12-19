use crate::dataflow::{
    context::SinkContext, operator::Sink,
    Data,
};
use serde::Deserialize;
use std::sync::Arc;

/// Takes input erdos stream and publishes to ROS topic using the provided message 
/// conversion function.

pub struct ToRosOperator<T, U: rosrust::Message>
where
    T: Data + for<'a> Deserialize<'a>,
{
    publisher: rosrust::Publisher<U>,
    to_ros_msg: Arc<dyn Fn(&T) -> U + Send + Sync>,
}

impl<T, U: rosrust::Message> ToRosOperator<T, U>
where
    T: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(topic: &str, to_ros_msg: F) -> Self 
    where
        F: 'static + Fn(&T) -> U + Send + Sync,
    {
        rosrust::init("publisher");
        Self {
            publisher: rosrust::publish(topic, 2).unwrap(),
            to_ros_msg: Arc::new(to_ros_msg),
        }
    }
}

impl<T, U: rosrust::Message> Sink<(), T> for ToRosOperator<T, U>
where
    T: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut SinkContext<()>, data: &T) {
        let timestamp = ctx.get_timestamp().clone();
        let msg = (self.to_ros_msg)(data);

        slog::info!(
            crate::TERMINAL_LOGGER,
            "ToRosOperator @ {:?}: Sending {:?}",
            timestamp,
            msg,
        );
        self.publisher.send(msg).unwrap();
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<()>) {}
}