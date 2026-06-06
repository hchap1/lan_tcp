use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::channel;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use bytes::Bytes;

use crate::error::Error;
use crate::error::Res;
use crate::networking::CHANNEL_SIZE;
use crate::networking::tcp::send_bytes;

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

    // Spawn a task to manage the client
    let join_handle = tokio::spawn(
        client_thread(tcp_stream)
    );

    Ok((
        send_input,
        recv_output,
        join_handle
    ))
}

/// Track a single TCPStream connected to a foreign server
async fn client_thread(
    connection: TcpStream,
    mut recv_input: Receiver<Bytes>,
    send_output: Sender<Bytes>
) -> Res<()> {
    
    // Split the connection into discrete read and write halves
    let (mut read_half, mut write_half) = connection.into_split();

    // TODO differentiate mpsc/tcp channel failure errors

    loop {
        tokio::select! {
            res = read_half.read_u32() => {

                // Parse the size of the incoming packet (32bit)
                let size = res.map_err(|_| Error::ChannelFailed)?;
                let mut buf = BytesMut::zeroed(size as usize);

                // Continue reading until the entire buffer is filled
                read_half.read_exact(&mut buf)
                    .await
                    .map_err(|_| Error::ChannelFailed)?;

                // Freeze the buffer (zero-copy) then output
                send_output.send(buf.freeze())
                    .await
                    .map_err(|_| Error::ChannelFailed)?;
            },

            res = recv_input.recv() => {

                // Parse the packet that the mpsc channel wishes to relay
                let bytes = res.ok_or(Error::ChannelFailed)?;

                // Send it over TCP
                send_bytes(&mut write_half, &bytes).await?;
            }

        };
    }

}
