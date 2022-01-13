use crate::dataflow::{context::SinkContext, operator::Sink, operators::ros::*, Data, Message};
use serde::Deserialize;
use std::sync::Arc;

/// Takes an input ERDOS stream and publishes to a ROS topic using the provided message conversion
/// function.
///
/// Conversion function should convert a Rust data type and return a ROS type which implements
/// the [`rosrust::Message`] trait.
///
/// # Example
/// The following example shows how to use a [`ToRosOperator`] with a conversion function which takes
/// a Rust [`i32`] and converts it to a ROS message with [`rosrust_msg::std_msgs::Int32`] data.
///
/// Assume that `source_stream` is an ERDOS stream sending the correct messages.
///
/// ```
/// fn erdos_int_to_ros_int(input: &Message<i32>) -> rosrust_msg::std_msgs::Int32 {
///     rosrust_msg::std_msgs::Int32 { data: input.data }
/// }
///
/// let ros_sink_config = OperatorConfig::new().name("ToRosInt32");
/// erdos::connect_sink(
///     move || -> ToRosOperator<i32, rosrust_msg::std_msgs::Int32> {
///         ToRosOperator::new("int_topic", erdos_int_to_ros_int)
///     },
///     || {},
///     ros_sink_config,
///     &source_stream,
/// );
/// ```

pub struct ToRosOperator<T, U: rosrust::Message>
where
    T: Data + for<'a> Deserialize<'a>,
{
    publisher: rosrust::Publisher<U>,
    to_ros_msg: Arc<dyn Fn(&Message<T>) -> U + Send + Sync>,
}

impl<T, U: rosrust::Message> ToRosOperator<T, U>
where
    T: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(topic: &str, to_ros_msg: F) -> Self
    where
        F: 'static + Fn(&Message<T>) -> U + Send + Sync,
    {
        rosrust::init("ERDOS Publisher");
        Self {
            publisher: rosrust::publish(topic, ROS_QUEUE_SIZE).unwrap(),
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
        let ros_msg = (self.to_ros_msg)(&Message::new_message(timestamp.clone(), data.clone()));

        tracing::trace!(
            "{} @ {:?}: Sending {:?}",
            ctx.get_operator_config().get_name(),
            timestamp,
            ros_msg,
        );
        // Publishes converted message on topic.
        self.publisher.send(ros_msg).unwrap();
    }

    fn on_watermark(&mut self, _ctx: &mut SinkContext<()>) {}
}
