use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;

use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::channel;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::error::Error;
use crate::error::Res;

pub async fn connect_client(addr: IpAddr, port: u16) -> Res<(
    Sender<Bytes>,
    Receiver<Bytes>,
    JoinHandle<Res<()>>
)> {

    // Create channel for relaying bytes between the node and the client
    let (
        send_input,
        recv_input
    ) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_SIZE);

    let (
        send_output,
        recv_output
    ) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_SIZE);


    // Attempt to retrieve IPV4 address of the server
    let addr = match addr {
        IpAddr::V4(ipv4) => SocketAddr::V4(SocketAddrV4::new(ipv4, port)),
        IpAddr::V6(ipv6) => Err(Error::CannotProcessIPV6)?
    };

    let tcp_stream = TcpStream::connect(addr)
        .await
        .map_err(|_| Error::FailedToEstablishTCPClient)?;

    Ok(())
}

async fn client_thread(tcp_stream: TcpStream) -> Res<()> {
    
}
