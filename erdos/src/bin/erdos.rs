use std::net::SocketAddr;

use clap::{App, Arg, SubCommand};
use erdos::{
    communication::DriverNotification,
    node::{LeaderNode, Resources, WorkerNode},
};
use futures::stream::StreamExt;
use signal_hook::consts::SIGINT;
use signal_hook_tokio::{Handle, Signals};
use tokio::{sync::mpsc, task::JoinHandle};

/// The return type to be used when setting up notification channels between
/// the signal handlers and the main Node loop.
type NotificationChannel = (Receiver<DriverNotification>, Handle, JoinHandle<()>);

fn setup_notification_channels() -> Result<NotificationChannel, Box<dyn std::error::Error>> {
    // Initialize the Signals handled by the CLI.
    let mut signals = Signals::new(&[SIGINT])?;
    let signals_handle = signals.handle();

    // Initialize a Broadcast channel to be used to talk to the nodes.
    let (driver_notification_tx, driver_notification_rx): (
        Sender<DriverNotification>,
        Receiver<DriverNotification>,
    ) = mpsc::channel(100);
    let signals_task = tokio::spawn(async move {
        while let Some(signal) = signals.next().await {
            match signal {
                SIGINT => {
                    driver_notification_tx
                        .send(DriverNotification::Shutdown)
                        .unwrap();
                }
                _ => unreachable!(),
            }
        }
    });
    Ok((driver_notification_rx, signals_handle, signals_task))
}

async fn cleanup_notification_channels(
    signals_handle: Handle,
    signals_task: JoinHandle<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Uninstall the signal handlers, and wait for the signal task to complete.
    signals_handle.close();
    signals_task.await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("start")
                .about("Initialize a Leader or a Worker node.")
                .arg(
                    Arg::with_name("leader")
                        .long("leader")
                        .takes_value(false)
                        .help("Initialize a Leader node."),
                )
                .arg(
                    Arg::with_name("address")
                        .long("address")
                        .default_value("0.0.0.0:4444")
                        .help("The address that is used by the Leader node."),
                )
                .arg(
                    Arg::with_name("num_gpus")
                        .long("num_gpus")
                        .default_value("0")
                        .help("The number of GPUs on this Worker node."),
                )
                .arg(
                    Arg::with_name("num_cpus")
                        .long("num_cpus")
                        .default_value("0")
                        .help("The number of CPUs on this Worker node."),
                ),
        )
        .get_matches();

    // Setup signal handling, and channels to communicate events to nodes.
    let (driver_notification_rx_channel, signals_handle, signals_task) =
        setup_notification_channels()?;

    if let Some(matches) = matches.subcommand_matches("start") {
        let leader_address: SocketAddr = matches.value_of("address").unwrap().parse()?;
        if matches.is_present("leader") {
            // Run a Leader node.
            let mut leader_node = LeaderNode::new(leader_address, driver_notification_rx_channel);
            match leader_node.run().await {
                Ok(()) => {
                    // Uninstall the signal handlers, and wait for the signal task to complete.
                    cleanup_notification_channels(signals_handle, signals_task).await?;
                }
                Err(error) => {
                    let _ = cleanup_notification_channels(signals_handle, signals_task).await;
                    return Err(error.into());
                }
            }
        } else {
            // Parse the Resources available at this WorkerNode.
            let num_gpus: usize = matches.value_of("num_gpus").unwrap().parse()?;
            let num_cpus: usize = matches.value_of("num_cpus").unwrap().parse()?;
            let worker_resources = Resources::new(num_cpus, num_gpus);

            // Run a WorkerNode.
            let mut worker_node = WorkerNode::new(
                0,
                leader_address,
                worker_resources,
                driver_notification_rx_channel,
            );
            match worker_node.run().await {
                Ok(()) => {
                    // Uninstall the signal handling mechanism.
                    cleanup_notification_channels(signals_handle, signals_task).await?;
                }
                Err(error) => {
                    let _ = cleanup_notification_channels(signals_handle, signals_task).await;
                    return Err(error.into());
                }
            }
        }
    }

    Ok(())
}
