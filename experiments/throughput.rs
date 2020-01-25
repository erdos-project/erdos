// TODO: refactor experiments
extern crate clap;
use clap::{App, Arg};

use std::process::{Child, Command, Stdio};

fn run_node(
    index: u32,
    data_addresses: String,
    control_addresses: String,
    messages: usize,
    message_size: usize,
) -> Child {
    Command::new("cargo")
        .arg("run")
        .arg("--release")
        .arg("--bin=experiment-throughput-driver")
        .arg("--")
        .arg(format!("--index={}", index))
        .arg(format!("--data-addresses={}", data_addresses))
        .arg(format!("--control-addresses={}", control_addresses))
        .arg(format!("--messages={}", messages))
        .arg(format!("--message-size={}", message_size))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
}

fn main() {
    let matches = App::new("ERDOS Throughput Experiments")
        .version("0.1")
        .author("ERDOS Team")
        .arg(
            Arg::with_name("mode")
                .index(1)
                .possible_values(&["inter-thread", "inter-process"]),
        )
        .arg(
            Arg::with_name("data-addresses")
                .short("a")
                .long("data-addresses")
                .default_value("127.0.0.1:9000,127.0.0.1:9001")
                .help("Comma separated list of socket addresses of all nodes"),
        )
        .arg(
            Arg::with_name("control-addresses")
                .short("c")
                .long("control-addresses")
                .default_value("127.0.0.1:9002,127.0.0.1:9003")
                .help("Comma separated list of socket addresses of all nodes"),
        )
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

    let mode: String = matches
        .value_of("mode")
        .unwrap()
        .parse::<String>()
        .expect("Unable to parse mode");
    let is_inter_thread = mode == "inter-thread";

    let data_addresses: String = if is_inter_thread {
        String::from("127.0.0.1:9000")
    } else {
        matches
            .value_of("data-addresses")
            .unwrap()
            .parse()
            .expect("Unable to parse addresses")
    };
    let control_addresses: String = if is_inter_thread {
        String::from("127.0.0.1:9001")
    } else {
        matches
            .value_of("control-addresses")
            .unwrap()
            .parse()
            .expect("Unable to parse addresses")
    };

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

    let mut node0 = run_node(
        0,
        data_addresses.clone(),
        control_addresses.clone(),
        num_messages,
        message_size,
    );
    if !is_inter_thread {
        run_node(
            1,
            data_addresses,
            control_addresses,
            num_messages,
            message_size,
        );
    }

    node0.wait().unwrap();
}
