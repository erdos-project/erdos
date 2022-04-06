use crate::dataflow::{context::SinkContext, operator::Sink, operators::ros::*, Data, Message};
use serde::Deserialize;
use std::sync::Arc;

/// Takes an input ERDOS stream and publishes to a ROS topic using the provided message conversion
/// function.
///
/// The conversion function transforms a [`Message`] into a ROS message which implements the
/// [`rosrust::Message`] trait.
///
/// See [`rosrust_msg`](https://lib.rs/crates/rosrust_msg),
/// the [ROS `std_msgs` package](http://wiki.ros.org/std_msgs),
/// and the [ROS `common_msgs` package](http://wiki.ros.org/common_msgs)
/// for a variety of supported and commonly-used ROS messages.
///
/// # Example
/// The following example shows how to use a [`ToRosOperator`] with a conversion function which
/// takes a Rust [`i32`] and converts it to a ROS message with
/// [`rosrust_msg::std_msgs::Int32`](http://docs.ros.org/en/api/std_msgs/html/msg/Int32.html)
/// data.
///
/// Assume that `source_stream` is an ERDOS stream sending the correct messages.
///
/// ```
/// # use erdos::{
/// #     dataflow::{Message, operators::ros::ToRosOperator, stream::IngestStream},
/// #     OperatorConfig
/// # };
/// #
/// # pub mod rosrust_msg {
/// #     pub mod std_msgs {
/// #         use std::io;
/// #
/// #         #[derive(Debug, Clone, PartialEq, Default)]
/// #         pub struct Int32 {
/// #             pub data: i32,
/// #         }
/// #
/// #         impl rosrust::Message for Int32 {
/// #             fn msg_definition() -> String { String::new() }
/// #             fn md5sum() -> String { String::new() }
/// #             fn msg_type() -> String { String::new() }
/// #         }
/// #
/// #         impl rosrust::RosMsg for Int32 {
/// #             fn encode<W: io::Write>(&self, mut w: W) -> io::Result<()> { Ok(()) }
/// #             fn decode<R: io::Read>(mut r: R) -> io::Result<Self> { Ok(Default::default()) }
/// #         }
/// #     }
/// # };
/// fn erdos_int_to_ros_int(input: &Message<i32>) -> Vec<rosrust_msg::std_msgs::Int32> {
///     match input.data() {
///         Some(x) => {
///             vec![rosrust_msg::std_msgs::Int32 {
///                 data: *x,
///             }]
///         }
///         None => vec![],
///     }
/// }
///
/// # let source_stream = IngestStream::new();
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
    to_ros_msg: Arc<dyn Fn(&Message<T>) -> Vec<U> + Send + Sync>,
}

impl<T, U: rosrust::Message> ToRosOperator<T, U>
where
    T: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(topic: &str, to_ros_msg: F) -> Self
    where
        F: 'static + Fn(&Message<T>) -> Vec<U> + Send + Sync,
    {
        Self {
            publisher: rosrust::publish(topic, ROS_QUEUE_SIZE).unwrap(),
            to_ros_msg: Arc::new(to_ros_msg),
        }
    }

    // Converts ERDOS message using conversion function and publishes all messages in
    // returned vector
    fn convert_and_publish(&mut self, ctx: &mut SinkContext<()>, erdos_msg: &Message<T>) {
        let ros_msg_vec = (self.to_ros_msg)(erdos_msg);

        for ros_msg in ros_msg_vec.into_iter() {
            tracing::trace!(
                "{} @ {:?}: Sending {:?}",
                ctx.operator_config().get_name(),
                ctx.timestamp().clone(),
                ros_msg,
            );
            // Publishes converted message on topic.
            self.publisher.send(ros_msg).unwrap();
        }
    }
}

impl<T, U: rosrust::Message> Sink<(), T> for ToRosOperator<T, U>
where
    T: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut SinkContext<()>, data: &T) {
        let timestamp = ctx.timestamp().clone();
        self.convert_and_publish(ctx, &Message::new_message(timestamp, data.clone()));
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<()>) {
        let timestamp = ctx.timestamp().clone();
        self.convert_and_publish(ctx, &Message::new_watermark(timestamp));
    }
}
