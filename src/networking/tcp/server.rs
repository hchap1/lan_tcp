use std::net::Ipv4Addr;
use std::net::SocketAddrV4;

use tokio::net::TcpListener;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;

use crate::error::Error;
use crate::error::Res;
use crate::networking::CHANNEL_SIZE;

pub type MPSCTx<T> = tokio::sync::mpsc::Receiver<T>;
pub type MPSCRx<T> = tokio::sync::mpsc::Receiver<T>;
pub type OneshotTx<T> = tokio::sync::oneshot::Receiver<T>;
pub type OneshotRx<T> = tokio::sync::oneshot::Receiver<T>;

pub async fn construct_server(port: u16) -> Res<()> {

    // Create channels for passing connections into send/recv
    let (
        send_readhalf,
        recv_readhalf
    ) = tokio::sync::mpsc::channel::<OwnedReadHalf>(CHANNEL_SIZE);

    let (
        send_writehalf,
        recv_writehalf
    ) = tokio::sync::mpsc::channel::<OwnedWriteHalf>(CHANNEL_SIZE);

    // Create termination signal channels
    let (
        detonator,
        recv_interrupt
    ) = tokio::sync::oneshot::channel::<()>();

    // Bind a TCP Listener on all available interfaces
    let listener = TcpListener::bind(
        SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port)
    ).await.map_err(|_| Error::FailedToEstablishTCPServer)?;

    Ok(())
}

/// Listens to TCP on all available interfaces
/// When a connection is received, spawn a task to handle it
/// Every instance of this task will push messages onto a queue
/// When a message is received, it will be sent to the servers own recv
/// Then, it will be sent to every other client except for the originator
pub async fn server_thread(recv_readhalf: MPSCRx<OwnedReadHalf>, recv_interrupt: OneshotRx<()>) {
    
}
