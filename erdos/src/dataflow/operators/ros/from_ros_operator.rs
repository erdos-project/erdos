use crate::dataflow::{
    operator::Source,
    operators::ros::*,
    stream::{WriteStream, WriteStreamT},
    Data, Message,
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

/// Subscribes to a ROS topic and outputs incoming messages to an ERDOS stream using the
/// provided message conversion function.
///
/// Conversion function should convert a ROS type which implements the [`rosrust::Message`]
/// trait and return a ERDOS [`Message`] containing data of a Rust data type. See [`rosrust_msg`]
/// for a variety of supported standard ROS messages.
///
/// # Example
/// The following example shows how to use a [`FromRosOperator`] with a conversion function
/// which takes a [`rosrust_msg::sensor_msgs::Image`] and returns an ERDOS message containing
/// [`Vec<u8>`] a vector of bytes.
///
/// ```
/// fn ros_image_to_bytes(input: &rosrust_msg::sensor_msgs::Image) -> Vector<Message<Vec<u8>>> {
///     vec![Message::new_message(Timestamp::Time(vec![0 as u64]), input.data)]
/// }
///
/// let ros_source_config = OperatorConfig::new().name("FromRosImage");
/// let ros_source = erdos::connect_source(
///     move || -> FromRosOperator<rosrust_msg::sensor_msgs::Image, Vec<u8>> {
///         FromRosOperator::new("image_topic", ros_image_to_bytes)
///     },
///     || {},
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

impl<T: rosrust::Message, U> Source<(), U> for FromRosOperator<T, U>
where
    U: Data + for<'a> Deserialize<'a>,
{
    fn run(&mut self, write_stream: &mut WriteStream<U>) {
        let from_ros_msg = self.from_ros_msg.clone();
        let write_stream_clone = Arc::new(Mutex::new(write_stream.clone()));

        let _subscriber_raii =
            rosrust::subscribe(self.topic.as_str(), ROS_QUEUE_SIZE, move |ros_msg: T| {
                let erdos_msg_vec = (from_ros_msg)(&ros_msg);

                for erdos_msg in erdos_msg_vec.into_iter() {
                    tracing::trace!("FromRosOperator: Received and Converted {:?}", erdos_msg,);
                    // Sends converted message on ERDOS stream.
                    write_stream_clone.lock().unwrap().send(erdos_msg).unwrap();
                }
            })
            .unwrap();

        rosrust::spin();
    }
}
