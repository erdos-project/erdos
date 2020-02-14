use clap::Arg;

use std::{
    f32, thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use erdos::dataflow::message::*;
use erdos::dataflow::{stream::WriteStreamT, OperatorConfig, ReadStream, WriteStream};
use erdos::node::Node;
use erdos::*;

static NUM_WARMUP_MESSAGES: usize = 0;

pub fn mean(x: &Vec<f32>) -> f32 {
    let mut sum = 0.0;
    for i in x.iter() {
        sum += *i;
    }
    sum / (x.len() as f32)
}

pub fn sort(x: &mut Vec<f32>) {
    x.sort_by(|a, b| a.partial_cmp(b).unwrap());
}

pub fn median(x: &mut Vec<f32>) -> f32 {
    sort(x);
    x[x.len() / 2]
}

pub fn min(x: &mut Vec<f32>) -> f32 {
    sort(x);
    x[0]
}

pub fn max(x: &mut Vec<f32>) -> f32 {
    *x.last().unwrap()
}

pub fn standard_dev(x: &Vec<f32>) -> f32 {
    let mean = mean(x);
    let var: f32 = x.iter().map(|&x| (x - mean) * (x - mean)).sum();
    let var = var / (x.len() as f32);
    var.sqrt()
}

pub fn get_time_us() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

pub struct SendOperator {
    write_stream: WriteStream<Vec<u8>>,
    num_messages: usize,
    message_size: usize,
}

impl SendOperator {
    pub fn new(config: OperatorConfig<(usize, usize)>, write_stream: WriteStream<Vec<u8>>) -> Self {
        Self {
            write_stream,
            num_messages: config.arg.0,
            message_size: config.arg.1,
        }
    }

    pub fn connect() -> WriteStream<Vec<u8>> {
        WriteStream::new()
    }

    pub fn run(&mut self) {
        println!("waiting for RecvOp to spawn");
        thread::sleep(Duration::from_secs(3)); // Wait for RecvOp to spawn
        println!("SendOp: sending warmup messages");
        for _ in 0..NUM_WARMUP_MESSAGES {
            self.write_stream
                .send(Message::new_message(
                    Timestamp::new(vec![0]),
                    vec![0u8; self.message_size],
                ))
                .expect("unable to unwrap!");
        }

        println!("Sending messages");
        let start = SystemTime::now();
        for _ in 0..self.num_messages {
            let data = vec![0u8; self.message_size];
            let time: u64 = get_time_us() as u64;
            self.write_stream
                .send(Message::new_message(Timestamp::new(vec![time]), data))
                .expect("unable to unwrap!");
        }
        let duration = SystemTime::now().duration_since(start).unwrap();

        println!(
            "Sent {} messages of size {} in {:.1} ms",
            self.num_messages,
            self.message_size,
            duration.as_micros() as f32 / 1e3
        );

        let throughput =
            (self.num_messages * self.message_size) as f32 * 1e6 / (duration.as_micros() as f32);

        let two: f32 = 2.0;
        print!("Send throughput: {:.2} B/s", throughput);
        print!("\t{:.2} KB/s", throughput / two.powi(10));
        print!("\t{:.2} MB/s", throughput / two.powi(20));
        print!("\t{:.2} GB/s", throughput / two.powi(30));

        print!("\n\n");
    }
}

#[derive(Clone)]
pub struct RecvState {
    start: SystemTime,
    num_received_messages: usize,
    num_received_warmup_messages: usize,
    num_total_messages: usize,
    avg_serialization_ms: f32,
    latencies: Vec<f32>,
}

impl RecvState {
    fn new(num_total_messages: usize, avg_serialization_ms: f32) -> Self {
        Self {
            start: SystemTime::now(),
            num_received_messages: 0,
            num_received_warmup_messages: 0,
            num_total_messages,
            avg_serialization_ms,
            latencies: Vec::with_capacity(num_total_messages),
        }
    }
}

pub struct RecvOperator {}

impl RecvOperator {
    pub fn new(config: OperatorConfig<(usize, usize)>, read_stream: ReadStream<Vec<u8>>) -> Self {
        let (num_total_messages, _message_size) = config.arg;

        // Profile serialization time
        /*
        println!("Profiling serialization time");
        let profile_msgs = 1000;
        let msg = Message::new_message(Timestamp::new(vec![0]), vec![0u8; message_size]);
        let start = SystemTime::now();
        for _ in 0..profile_msgs {
        let serialized = bincode::serialize(&msg).unwrap();
        let bytes: Message<Vec<u8>> = bincode::deserialize(&serialized).unwrap();
        }
        let serialization_time = SystemTime::now().duration_since(start).unwrap();
        let avg_serialization_ms =
        serialization_time.as_micros() as f32 / 1000.0 / profile_msgs as f32;
        println!("Average serialization time: {} ms", avg_serialization_ms);
        */
        let avg_serialization_ms = 0.0;

        let state = RecvState::new(num_total_messages, avg_serialization_ms);
        read_stream
            .add_state(state)
            .add_callback(Self::msg_callback);
        println!("Ready to receive messages");
        Self {}
    }

    pub fn connect(_read_stream: &ReadStream<Vec<u8>>) {}

    pub fn run(&self) {}

    pub fn msg_callback(timestamp: Timestamp, msg: Vec<u8>, state: &mut RecvState) {
        if state.num_received_warmup_messages < NUM_WARMUP_MESSAGES {
            state.num_received_warmup_messages += 1;
            return;
        }
        if state.num_received_messages == 0 {
            state.start = SystemTime::now();
        }
        state.num_received_messages += 1;

        let time_us: u64 = get_time_us() as u64;
        let latency_us = time_us - timestamp.time[0];
        let latency_ms = latency_us as f32 / 1000.0;
        state.latencies.push(latency_ms);

        if state.num_received_messages == state.num_total_messages {
            let duration = SystemTime::now().duration_since(state.start).unwrap();
            eprintln!("Done receiving messages");

            println!(
                "Received {} messages of size {} in {:.1} ms",
                state.num_total_messages,
                msg.len(),
                duration.as_micros() as f32 / 1e3
            );

            let throughput =
                (state.num_total_messages * msg.len()) as f32 * 1e6 / (duration.as_micros() as f32);
            let two: f32 = 2.0;
            print!("Receive throughput: {:.2} B/s", throughput);
            print!("\t{:.2} KB/s", throughput / two.powi(10));
            print!("\t{:.2} MB/s", throughput / two.powi(20));
            print!("\t{:.2} GB/s", throughput / two.powi(30));

            print!("\n\n");

            println!("Latency statistics:");
            println!("\tmean: {} ms", mean(&state.latencies));
            println!("\tmedian: {} ms", median(&mut state.latencies));
            println!(
                "\tstandard deviation: {} ms",
                standard_dev(&state.latencies)
            );
            println!("\tmin: {} ms", min(&mut state.latencies));
            println!("\tmax: {} ms", max(&mut state.latencies));

            println!(
                "On average, serialization is responsible for {}% of latency",
                state.avg_serialization_ms / mean(&state.latencies) * 100.0
            )
        }
    }
}

pub struct PullRecvOp {
    num_total_messages: usize,
    message_size: usize,
    read_stream: ReadStream<Vec<u8>>,
}

impl PullRecvOp {
    pub fn new(config: OperatorConfig<(usize, usize)>, read_stream: ReadStream<Vec<u8>>) -> Self {
        let (num_total_messages, message_size) = config.arg;
        Self {
            num_total_messages,
            message_size,
            read_stream,
        }
    }

    pub fn connect(_read_stream: &ReadStream<Vec<u8>>) {}

    pub fn run(&self) {
        println!("RecvOp: receiving warmup messages");
        for _ in 0..NUM_WARMUP_MESSAGES {
            self.read_stream.read();
        }

        let mut msg_times: Vec<(u64, SystemTime)> = Vec::with_capacity(self.num_total_messages);

        println!("RecvOp: receiving messages");
        let start = SystemTime::now();
        for _ in 0..self.num_total_messages {
            match self.read_stream.read().unwrap() {
                Message::TimestampedData(msg) => {
                    msg_times.push((msg.timestamp.time[0], SystemTime::now()));
                }
                _ => (),
            };
        }
        let duration = SystemTime::now().duration_since(start).unwrap();
        thread::sleep(Duration::from_millis(100)); // Sleep to avoid junk text from printing concurrently as SendOp for inter-thread mode

        let mut latencies: Vec<f32> = Vec::with_capacity(self.num_total_messages);
        for i in 0..msg_times.len() {
            let (send_us, recv_time) = msg_times[i];
            let recv_time = recv_time.duration_since(UNIX_EPOCH).unwrap();
            let latency_us = recv_time.as_micros() as u64 - send_us;
            let latency_ms = latency_us as f32 / 1000.0;
            latencies.push(latency_ms);
        }

        println!(
            "Received {} messages of size {} in {} ms",
            self.num_total_messages,
            self.message_size,
            duration.as_millis()
        );

        let throughput = (self.num_total_messages * self.message_size) as f32 * 1e3
            / (duration.as_millis() as f32);
        let two: f32 = 2.0;
        print!("Throughput: {:.2} B/s", throughput);
        print!("\t{:.2} KB/s", throughput / two.powi(10));
        print!("\t{:.2} MB/s", throughput / two.powi(20));
        print!("\t{:.2} GB/s", throughput / two.powi(30));

        println!();
        println!();

        println!("Latency statistics:");
        println!("\tmean: {} ms", mean(&latencies));
        println!("\tmedian: {} ms", median(&mut latencies));
        println!("\tstandard deviation: {} ms", standard_dev(&latencies));
        println!("\tmin: {} ms", min(&mut latencies));
        println!("\tmax: {} ms", max(&mut latencies));
    }
}

fn main() {
    let args = erdos::new_app("ERDOS");
    let matches = args
        .arg(
            Arg::with_name("messages")
                .short("m")
                .long("messages")
                .default_value("1000")
                .help("Number of messages to send"),
        )
        .arg(
            Arg::with_name("message size")
                .short("s")
                .long("message-size")
                .default_value("1")
                .help("Size of message in bytes"),
        )
        .get_matches();

    let node_config = Configuration::from_args(&matches);
    let recv_node = node_config.data_addresses.len() - 1;
    let mut node = Node::new(node_config);

    let num_messages: usize = matches
        .value_of("messages")
        .unwrap()
        .parse()
        .expect("Unable to parse number of messages");
    let message_size: usize = matches
        .value_of("message size")
        .unwrap()
        .parse()
        .expect("Unable to parse message size");

    let send_config = OperatorConfig::new("SendOperator", (num_messages, message_size), true, 0);
    let s = connect_1_write!(SendOperator, send_config);
    let recv_config = OperatorConfig::new(
        "RecvOperator",
        (num_messages, message_size),
        true,
        recv_node,
    );
    // connect_0_write!(RecvOperator, recv_config, s);
    connect_0_write!(PullRecvOp, recv_config, s);

    node.run();
}
