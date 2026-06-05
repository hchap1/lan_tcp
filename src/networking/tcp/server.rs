use std::sync::Arc;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast::channel;
use tokio::sync::Semaphore;
use tokio::sync::OwnedSemaphorePermit;

use bytes::Bytes;
use bytes::BytesMut;
use tokio::task::JoinHandle;

use crate::error::Error;
use crate::error::Res;
use crate::networking::CHANNEL_SIZE;

pub type MPSCTx<T> = tokio::sync::mpsc::Receiver<T>;
pub type MPSCRx<T> = tokio::sync::mpsc::Receiver<T>;

#[derive(Clone)]
enum Relay {
    Internal(Bytes),
    External(SocketAddr, Bytes)
}

pub async fn construct_server(port: u16, max_connections: usize) -> Res<MPSCTx<Bytes>> {

    // Create channel for relaying bytes around the server
    let (
        send_bytes,
        recv_bytes
    ) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_SIZE);

    // Bind a TCP Listener on all available interfaces
    let listener = TcpListener::bind(
        SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port)
    ).await.map_err(|_| Error::FailedToEstablishTCPServer)?;

    Ok(recv_bytes)
}

/// Handle an individual TCP connection
pub async fn handle_connection(
    connection: TcpStream,
    broadcast_sender: tokio::sync::broadcast::Sender<Relay>,
    mut broadcast_receiver: tokio::sync::broadcast::Receiver<Relay>,
    permit: OwnedSemaphorePermit
) -> Res<()> {

    // Find the address of the remote connection prior to splitting
    let addr = connection.peer_addr().map_err(|_| Error::ChannelFailed)?;
    let (mut read_half, mut write_half) = connection.into_split();

    loop {
        tokio::select! {
            res = read_half.read_u32() => {

                // Parse the size of the incoming packet (32bit)
                let size = res.map_err(|_| Error::ChannelFailed)?;
                let mut buf = BytesMut::zeroed(size as usize);

                // Continue reading until the entire buffer is filled
                read_half.read_exact(&mut buf).await.map_err(|_| Error::ChannelFailed)?;

                // Freeze the buffer (zero-copy) then broadcast
                // This bypasses the main thread entirely to avoid bottleneck
                let broadcast = Relay::External(addr, buf.freeze());
                broadcast_sender.send(broadcast).map_err(|_| Error::BroadcastFailed)?;
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

                if let Some(bytes) = maybe_bytes {

                    // BE (big endian) representation of byte array size
                    let be_len_repr = (bytes.len() as u32).to_be_bytes();

                    // Write the length of the bytes followed by the bytes
                    write_half.write_all(&be_len_repr).await.map_err(|_| Error::ChannelFailed)?;
                    write_half.write_all(&bytes).await.map_err(|_| Error::ChannelFailed)?;
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
pub async fn server_thread(listener: TcpListener, recv_bytes: MPSCRx<Bytes>, max_connections: usize) -> Res<()> {

    // Create a broadcast system so that tasks can contact one another
    let (broadcaster, broadcast_receiver) = channel(CHANNEL_SIZE);
    let mut tasks: Vec<JoinHandle<Res<()>>> = vec![];

    // Semaphore to limit number of tasks
    let semaphore = Arc::new(Semaphore::new(max_connections));
    
    loop {
        tokio::select! {
            maybe_connection = listener.accept() => match maybe_connection {
                Ok((connection, _)) => {

                    // Limit the number of active tasks
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
                },

                Err(_) => Err(Error::FailedToEstablishTCPConnection)?
            }
        }
    }
}
