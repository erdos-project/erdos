use crate::dataflow::{
    operator::{OperatorConfig, Source},
    operators::ros::*,
    stream::{WriteStream, WriteStreamT},
    Data, Message,
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

/// Subscribes to a ROS topic and outputs incoming messages to an ERDOS stream using the
/// provided message conversion function.
///
/// The conversion function transforms a ROS message implementing the [`rosrust::Message`] trait
/// into an ERDOS [`Message`].
///
/// See [`rosrust_msg`](https://lib.rs/crates/rosrust_msg),
/// the [ROS `std_msgs` package](http://wiki.ros.org/std_msgs),
/// and the [ROS `common_msgs` package](http://wiki.ros.org/common_msgs)
/// for a variety of supported and commonly-used ROS messages.
///
/// # Example
/// The following example shows how to use a [`FromRosOperator`] with a conversion function
/// which takes a
/// [`rosrust_msg::sensor_msgs::Image`](http://docs.ros.org/en/api/sensor_msgs/html/msg/Image.html)
/// and returns an ERDOS message containing [`Vec<u8>`] a vector of bytes.
///
/// ```
/// # use erdos::{
/// #     dataflow::{Message, operators::ros::FromRosOperator, Timestamp},
/// #     OperatorConfig,
/// # };
/// #
/// # pub mod rosrust_msg {
/// #     pub mod sensor_msgs {
/// #         use std::io;
/// #
/// #         #[derive(Debug, Clone, PartialEq, Default)]
/// #         pub struct Image {
/// #             pub data: Vec<u8>,
/// #         }
/// #
/// #         impl rosrust::Message for Image {
/// #             fn msg_definition() -> String { String::new() }
/// #             fn md5sum() -> String { String::new() }
/// #             fn msg_type() -> String { String::new() }
/// #         }
/// #
/// #         impl rosrust::RosMsg for Image {
/// #             fn encode<W: io::Write>(&self, mut w: W) -> io::Result<()> { Ok(()) }
/// #             fn decode<R: io::Read>(mut r: R) -> io::Result<Self> { Ok(Default::default()) }
/// #         }
/// #     }
/// # };
/// fn ros_image_to_bytes(input: &rosrust_msg::sensor_msgs::Image) -> Vec<Message<Vec<u8>>> {
///     vec![Message::new_message(Timestamp::Time(vec![0 as u64]), input.data.clone())]
/// }
///
/// let ros_source_config = OperatorConfig::new().name("FromRosImage");
/// let ros_source = erdos::connect_source(
///     move || -> FromRosOperator<rosrust_msg::sensor_msgs::Image, Vec<u8>> {
///         FromRosOperator::new("image_topic", ros_image_to_bytes)
///     },
///     ros_source_config,
/// );
/// ```

#[derive(Clone)]
pub struct FromRosOperator<T: rosrust::Message, U>
where
    U: Data + for<'a> Deserialize<'a>,
{
    topic: String,
    from_ros_msg: Arc<dyn Fn(&T) -> Vec<Message<U>> + Send + Sync>,
}

impl<T: rosrust::Message, U> FromRosOperator<T, U>
where
    U: Data + for<'a> Deserialize<'a>,
{
    pub fn new<F>(topic: &str, from_ros_msg: F) -> Self
    where
        F: 'static + Fn(&T) -> Vec<Message<U>> + Send + Sync,
    {
        Self {
            topic: topic.to_string(),
            from_ros_msg: Arc::new(from_ros_msg),
        }
    }
}

impl<T: rosrust::Message, U> Source<U> for FromRosOperator<T, U>
where
    U: Data + for<'a> Deserialize<'a>,
{
    fn run(&mut self, config: &OperatorConfig, write_stream: &mut WriteStream<U>) {
        let from_ros_msg = self.from_ros_msg.clone();
        let config_clone = config.clone();
        let write_stream_clone = Arc::new(Mutex::new(write_stream.clone()));

        let _subscriber_raii =
            rosrust::subscribe(self.topic.as_str(), ROS_QUEUE_SIZE, move |ros_msg: T| {
                let erdos_msg_vec = (from_ros_msg)(&ros_msg);

                for erdos_msg in erdos_msg_vec.into_iter() {
                    tracing::trace!(
                        "{}: Received and Converted {:?}",
                        config_clone.get_name(),
                        erdos_msg,
                    );
                    // Sends converted message on ERDOS stream.
                    write_stream_clone.lock().unwrap().send(erdos_msg).unwrap();
                }
            })
            .unwrap();

        rosrust::spin();
    }
}
