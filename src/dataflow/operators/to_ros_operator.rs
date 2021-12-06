use crate::dataflow::{
    context::SinkContext, operator::Sink,
    Data,
};
use serde::Deserialize;

pub struct ToRosOperator {
    publisher: rosrust::Publisher<rosrust_msg::std_msgs::String>,
}

impl ToRosOperator
{
    pub fn new() -> Self {
        env_logger::init();
        rosrust::init("publisher");
        Self {
            publisher: rosrust::publish("chatter", 1).unwrap(),
        }
    }
}

impl<D1> Sink<(), D1> for ToRosOperator
where
    D1: Data + for<'a> Deserialize<'a>,
{
    fn on_data(&mut self, ctx: &mut SinkContext<()>, data: &D1) {
        let timestamp = ctx.get_timestamp().clone();
        let msg = rosrust_msg::std_msgs::String {
            data: format!("{:?}", data),
        };
        slog::info!(
            crate::TERMINAL_LOGGER,
            "ToRosOperator @ {:?}: Sending {:?}",
            timestamp,
            msg
        );
        let result = self.publisher.send(msg);
        // match result {
        //     Ok(v) => println!("nice"),
        //     Err(e) => println!("Error {}", e),
        // }
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<()>) {}
}