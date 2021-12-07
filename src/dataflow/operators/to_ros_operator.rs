use crate::dataflow::{
    context::SinkContext, operator::Sink,
    Data,
};
use serde::Deserialize;

pub struct ToRosOperator {
    publisher: rosrust::Publisher<rosrust::RawMessage>,
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
        let custom = rosrust::RawMessage(vec![1, 2, 3]);
        slog::info!(
            crate::TERMINAL_LOGGER,
            "ToRosOperator @ {:?}: Sending {:?}",
            timestamp,
            custom.0
        );
        let result = self.publisher.send(custom);
        // match result {
        //     Ok(v) => println!("nice"),
        //     Err(e) => println!("Error {}", e),
        // }
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<()>) {}
}