use std::net::SocketAddr;

use clap::{App, Arg, SubCommand};
use erdos::{
    communication::DriverNotification,
    node::{LeaderNode, WorkerNode},
};
use futures::stream::StreamExt;
use signal_hook::consts::SIGINT;
use signal_hook_tokio::{Handle, Signals};
use tokio::{
    sync::broadcast::{self, Receiver, Sender},
    task::JoinHandle,
};

/// The return type to be used when setting up notification channels between
/// the signal handlers and the main Node loop.
type NotificationChannel = (Receiver<DriverNotification>, Handle, JoinHandle<()>);

fn setup_notification_channels(
) -> Result<NotificationChannel, Box<dyn std::error::Error>> {
    // Initialize the Signals handled by the CLI.
    let mut signals = Signals::new(&[SIGINT])?;
    let signals_handle = signals.handle();

    // Initialize an MPSC channel to be used to talk to the nodes.
    let (driver_notification_tx_channel, driver_notification_rx_channel): (
        Sender<DriverNotification>,
        Receiver<DriverNotification>,
    ) = broadcast::channel(100);
    let signals_task = tokio::spawn(async move {
        while let Some(signal) = signals.next().await {
            match signal {
                SIGINT => {
                    driver_notification_tx_channel
                        .send(DriverNotification::Shutdown)
                        .unwrap();
                }
                _ => unreachable!(),
            }
        }
    });
    Ok((driver_notification_rx_channel, signals_handle, signals_task))
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
            // Run a WorkerNode.
            let mut worker_node = WorkerNode::new(leader_address, driver_notification_rx_channel);
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
