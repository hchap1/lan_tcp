use std::net::IpAddr;
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
use crate::networking::node::RecvPacket;
use crate::networking::node::SendPacket;
use crate::networking::tcp::send_bytes;
use crate::networking::node::Destination;
use crate::networking::tcp::Headable;

/// Contains DESTINATIONS (origin) PACKET
/// If destinations is empty, it is intended for every node
#[derive(Clone)]
enum Relay {
    Internal(Vec<Ipv4Addr>, Bytes),
    External(Vec<Ipv4Addr>, SocketAddr, Bytes)
}

/// Starts a server process managing TCP clients efficiently
/// Exposes MPSC channels for bytes in, bytes out
pub async fn construct_server(port: u16, max_connections: usize) -> Res<(
    Sender<SendPacket>,
    Receiver<RecvPacket>,
    JoinHandle<Res<()>>,
    Arc<Semaphore>
)> {

    // Create channel for relaying bytes between the node and server
    let (
        send_input,
        recv_input
    ) = tokio::sync::mpsc::channel::<SendPacket>(CHANNEL_SIZE);

    let (
        send_output,
        recv_output
    ) = tokio::sync::mpsc::channel::<RecvPacket>(CHANNEL_SIZE);

    // Bind a TCP Listener on all available interfaces
    let listener = TcpListener::bind(
        SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port)
    ).await.map_err(|_| Error::FailedToEstablishTCPServer)?;

    // Start a task managing
    // - Client connections
    // - Client tasks

    // Semaphore to limit number of tasks
    let semaphore = Arc::new(Semaphore::new(max_connections));

    let join_handle = tokio::spawn(
        server_task(
            listener,
            recv_input,
            send_output,
            semaphore.clone()
        )
    );

    Ok((send_input, recv_output, join_handle, semaphore))
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
    let ipv4 = match addr.ip() {
        IpAddr::V4(ipv4) => ipv4,
        IpAddr::V6(_) => Err(Error::CannotProcessIPV6)?
    };
    let (mut read_half, mut write_half) = connection.into_split();

    loop {
        tokio::select! {
            res = read_half.read_u32() => {

                // Parse the size of the incoming packet (32bit)
                let size = res.map_err(|_| Error::TcpChannelFailed)?;
                let mut buf = BytesMut::zeroed(size as usize);

                // Parse the number of addresses (8bit)
                let mut address_count = BytesMut::zeroed(1usize);
                read_half.read_exact(&mut address_count)
                    .await.map_err(|_| Error::TcpChannelFailed)?;

                // Parse each address in the packet
                let address_count = address_count[0] as usize;
                let mut addresses = Vec::new();
                for _ in 0..address_count {
                    let mut this_address = BytesMut::zeroed(4usize);
                    read_half.read_exact(&mut this_address)
                        .await.map_err(|_| Error::TcpChannelFailed)?;
                    addresses.push(Ipv4Addr::new(
                        this_address[0],
                        this_address[1],
                        this_address[2],
                        this_address[3]
                    ));
                }

                // Continue reading until the entire buffer is filled
                read_half.read_exact(&mut buf)
                    .await.map_err(|_| Error::TcpChannelFailed)?;

                // Freeze the buffer (zero-copy) then broadcast
                // This bypasses the main thread entirely to avoid bottleneck
                let broadcast = Relay::External(addresses, addr, buf.freeze());
                broadcast_sender.send(broadcast)
                    .map_err(|_| Error::BroadcastFailed)?;
            },

            res = broadcast_receiver.recv() => {

                // Parse the packet that the broadcast channel wishes to relay
                let relay = match res {
                    Ok(r) => r,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => Err(Error::BroadcastFailed)?
                };

                // Ignore packets from self
                let (maybe_bytes, maybe_author) = match relay {

                    // If from server, cannot be from self
                    Relay::Internal(destinations, bytes) => match destinations.len() {
                        0usize => (Some(bytes), Some(Ipv4Addr::new(1, 1, 1, 1))),
                        _ => if destinations[0] == Ipv4Addr::new(1, 1, 1, 1) {

                            // The packet is intended only for the server
                            (None, None)
                        } else {

                            if destinations.contains(&ipv4) {

                                // The packet was intended for this client
                                (Some(bytes), Some(Ipv4Addr::new(1, 1, 1, 1)))
                            } else {
                                (None, None)
                            }
                        }
                    }

                    Relay::External(destinations, author, bytes) => {

                        let author_ipv4 = match author.ip() {
                            IpAddr::V4(ipv4) => ipv4,
                            IpAddr::V6(_) => Err(Error::CannotProcessIPV6)?
                        };

                        if author == addr {
                            (None, None)
                        } else {

                            // If the packet was written by another
                            match destinations.len() {
                                0usize => (Some(bytes), Some(author_ipv4)),
                                _ => if destinations[0] == Ipv4Addr::new(1, 1, 1, 1) {

                                    // The packet is intended only for the server
                                    (None, None)
                                } else {

                                    if destinations.contains(&ipv4) {

                                        // The packet was intended for this client
                                        (Some(bytes), Some(author_ipv4))
                                    } else {
                                        (None, None)
                                    }
                                }
                            }
                        }
                    }

                };

                // If the bytes weren't from self, send them on the channel
                if let (Some(bytes), Some(author_ipv4)) = (maybe_bytes, maybe_author) {
                    let recv_packet = RecvPacket {
                        data: bytes,
                        origination: author_ipv4
                    };
                    send_bytes(&mut write_half, &recv_packet).await?;
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
    mut recv_input: Receiver<SendPacket>,
    send_output: Sender<RecvPacket>,
    semaphore: Arc<Semaphore>
) -> Res<()> {

    let my_ip = udp_discovery::server::Server::find_suitable_ipv4()
        .await.map_err(|_| Error::FailedToEstablishTCPServer)?;

    // Create a broadcast system so that tasks can contact one another
    let (broadcaster, mut broadcast_receiver) = channel(CHANNEL_SIZE);
    let mut tasks: Vec<JoinHandle<Res<()>>> = vec![];

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
            maybe_send_packet = recv_input.recv() => {
                let send_packet = maybe_send_packet.ok_or(Error::MpscChannelFailed)?;

                // Parse destination
                let destinations = match send_packet.destination.clone() {
                    Destination::All => vec![],
                    Destination::Server => vec![Ipv4Addr::new(1, 1, 1, 1)],
                    Destination::Single(addr) => vec![addr],
                    Destination::Multiple(addrs) => addrs
                };

                broadcaster.send(Relay::Internal(destinations, send_packet.body().clone()))
                    .map_err(|_| Error::BroadcastFailed)?;
                None
            },

            // Read the broadcast channel to output to the Node
            maybe_relay = broadcast_receiver.recv() => {
                let relay = match maybe_relay {
                    Ok(r) => r,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => Err(Error::BroadcastFailed)?
                };
                match relay {
                    
                    // The servers own message should always be ignored
                    Relay::Internal(_, _) => None,

                    // An incoming message from some client
                    Relay::External(destinations, author, bytes) => {
                        let packet = Some(
                            RecvPacket {
                                data: bytes,
                                origination: match author.ip() {
                                    IpAddr::V4(addr) => addr,
                                    _ => Err(Error::CannotProcessIPV6)?
                                }
                            }
                        );

                        match destinations.len() {
                            0usize => packet,
                            _ => if destinations.contains(&Ipv4Addr::new(1, 1, 1, 1)) {
                                packet
                            } else if destinations.contains(&my_ip) {
                                packet
                            } else {
                                None
                            }
                        }
                    }
                }
            }
        };

        // If something caused output to be produced, dispatch it
        if let Some(bytes) = output_to_node {
            send_output.send(bytes).await.map_err(|_| Error::MpscChannelFailed)?;
        }
    }
}
