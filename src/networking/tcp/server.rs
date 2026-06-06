use std::sync::Arc;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;
use tokio::sync::broadcast::channel;
use tokio::sync::Semaphore;
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinHandle;

use bytes::Bytes;
use bytes::BytesMut;

use crate::error::Error;
use crate::error::Res;
use crate::networking::CHANNEL_SIZE;
use crate::networking::tcp::send_bytes;

#[derive(Clone)]
enum Relay {
    Internal(Bytes),
    External(SocketAddr, Bytes)
}

/// Starts a server process managing TCP clients efficiently
/// Exposes MPSC channels for bytes in, bytes out
pub async fn construct_server(port: u16, max_connections: usize) -> Res<(
    Sender<Bytes>,
    Receiver<Bytes>,
    JoinHandle<Res<()>>
)> {

    // Create channel for relaying bytes between the node and server
    let (
        send_input,
        recv_input
    ) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_SIZE);

    let (
        send_output,
        recv_output
    ) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_SIZE);

    // Bind a TCP Listener on all available interfaces
    let listener = TcpListener::bind(
        SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port)
    ).await.map_err(|_| Error::FailedToEstablishTCPServer)?;

    // Start a task managing
    // - Client connections
    // - Client tasks

    let join_handle = tokio::spawn(
        server_task(
            listener,
            recv_input,
            send_output,
            max_connections
        )
    );

    Ok((send_input, recv_output, join_handle))
}

/// Handle an individual TCP connection
async fn handle_connection(
    connection: TcpStream,
    broadcast_sender: tokio::sync::broadcast::Sender<Relay>,
    mut broadcast_receiver: tokio::sync::broadcast::Receiver<Relay>,
    _permit: OwnedSemaphorePermit
) -> Res<()> {

    // Find the address of the remote connection prior to splitting
    let addr = connection.peer_addr().map_err(|_| Error::TcpChannelFailed)?;
    let (mut read_half, mut write_half) = connection.into_split();

    loop {
        tokio::select! {
            res = read_half.read_u32() => {

                // Parse the size of the incoming packet (32bit)
                let size = res.map_err(|_| Error::TcpChannelFailed)?;
                let mut buf = BytesMut::zeroed(size as usize);

                // Continue reading until the entire buffer is filled
                read_half.read_exact(&mut buf)
                    .await.map_err(|_| Error::TcpChannelFailed)?;

                // Freeze the buffer (zero-copy) then broadcast
                // This bypasses the main thread entirely to avoid bottleneck
                let broadcast = Relay::External(addr, buf.freeze());
                broadcast_sender.send(broadcast)
                    .map_err(|_| Error::BroadcastFailed)?;
            },

            res = broadcast_receiver.recv() => {

                // Parse the packet that the broadcast channel wishes to relay
                let relay = res.map_err(|_| Error::BroadcastFailed)?;

                // Ignore packets from self
                let maybe_bytes = match relay {
                    Relay::Internal(bytes) => Some(bytes),
                    Relay::External(author, bytes) =>
                        if author == addr { None }
                        else { Some(bytes) }
                };

                // If the bytes weren't from self, send them on the channel
                if let Some(bytes) = maybe_bytes {
                    send_bytes(&mut write_half, &bytes).await?;
                }
            }
        }
    }
}

/// Listens to TCP on all available interfaces
/// When a connection is received, spawn a task to handle it
/// Every instance of this task will push messages onto a queue
/// When a message is received, it will be sent to the servers own recv
/// Then, it will be sent to every other client except for the originator
pub async fn server_task(
    listener: TcpListener,
    mut recv_input: Receiver<Bytes>,
    send_output: Sender<Bytes>,
    max_connections: usize
) -> Res<()> {

    // Create a broadcast system so that tasks can contact one another
    let (broadcaster, mut broadcast_receiver) = channel(CHANNEL_SIZE);
    let mut tasks: Vec<JoinHandle<Res<()>>> = vec![];

    // Semaphore to limit number of tasks
    let semaphore = Arc::new(Semaphore::new(max_connections));
    
    loop {
        let output_to_node = tokio::select! {

            // Listen for new connections
            maybe_connection = listener.accept() => match maybe_connection {
                Ok((connection, _)) => {

                    // Limit the number of active tasks
                    // This does pause the ability of the server to participate
                    // Will need to be fixed later. TODO
                    let permit = Arc::clone(&semaphore)
                        .acquire_owned()
                        .await
                        .map_err(|_| Error::UnableToAcquirePermit)?;

                    // Spawn a new task to handle the acquired connection
                    tasks.push(
                        tokio::spawn(
                            handle_connection(
                                connection,
                                broadcaster.clone(),
                                broadcaster.subscribe(),
                                permit
                            )
                        )
                    );

                    None
                },

                Err(_) => Err(Error::FailedToEstablishTCPConnection)?
            },

            // Check if the Node wishes to send any messages
            // If so, broadcast them to all active clients
            maybe_bytes = recv_input.recv() => {
                let bytes = maybe_bytes.ok_or(Error::MpscChannelFailed)?;
                broadcaster.send(Relay::Internal(bytes))
                    .map_err(|_| Error::BroadcastFailed)?;
                None
            },

            // Read the broadcast channel to output to the Node
            maybe_relay = broadcast_receiver.recv() => {
                let relay = maybe_relay.map_err(|_| Error::BroadcastFailed)?;
                match relay {
                    
                    // The servers own message
                    Relay::Internal(_) => None,

                    // An incoming message from some client
                    Relay::External(_, bytes) => Some(bytes)
                }
            }
        };

        // If something caused output to be produced, dispatch it
        if let Some(bytes) = output_to_node {
            send_output.send(bytes).await.map_err(|_| Error::MpscChannelFailed)?;
        }
    }
}
