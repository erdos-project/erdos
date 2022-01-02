use crate::dataflow::{
    context::SinkContext, operator::Sink,
    Data,
};
use serde::Deserialize;
use std::sync::Arc;

/// Takes an input ERDOS stream and publishes to a ROS topic using the provided message conversion 
/// function.
/// 
/// Conversion function should convert a Rust data type and return a ROS type which implements 
/// the [`rosrust::Message`] trait.
/// 
/// # Example
/// The following example shows a conversion function which takes a Rust [`i32`] and converts it
/// to a ROS message with [`rosrust_msg::std_msgs::Int32`] data. 
/// 
/// ```
/// fn erdos_to_ros(input: &i32) -> rosrust_msg::std_msgs::Int32 {
///     rosrust_msg::std_msgs::Int32 { data: input.data }
/// }
/// ```

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

        tracing::info!(
            "{} @ {:?}: Sending {:?}",
            ctx.get_operator_config().get_name(),
            timestamp,
            msg,
        );
        // Publishes converted message on topic.
        self.publisher.send(msg).unwrap();
    }

    fn on_watermark(&mut self, _ctx: &mut SinkContext<()>) {}
}