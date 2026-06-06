use std::net::IpAddr;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;

use crate::error::Res;
use crate::networking::tcp::server;

pub struct Node {

    // Port and code designated for this application
    port: u16,
    identifier: &'static str,

    // Thread processing TCP communication
    tcp_handle: JoinHandle<Res<()>>,

    // For servers only, keeps the UDP handler alive
    udp_handle: Option<udp_discovery::server::Server>,

    // MPSC sender for handing bytes to be forwarded
    outgoing_queue: Sender<Bytes>,

    // MPSC receiver for dequeuing incoming bytes
    incoming_queue: Receiver<Bytes>
}

impl Node {

    /// Construct the threads and callback structure for a Server
    /// Then package them together with UDP advertisement into a Node
    pub async fn spawn_server(
        identifier: &'static str, port: u16, max_connections: usize
    ) -> Res<Self> {

        // 1 Start TCP server task
        let (
            outgoing_queue,
            incoming_queue,
            tcp_handle
        ) = server::construct_server(port, max_connections).await?;

        // 2 Start responding on UDP
        let udp_handle = Some(
            udp_discovery::server::Server::spawn(identifier, port).await
        );

        // 3 Package handles and return
        Ok(Node {
            port,
            identifier,
            tcp_handle,
            udp_handle,
            outgoing_queue,
            incoming_queue
        })
    }

    /// After discovering a Server, build the recv and send threads
    /// Then package them together with MPSC into a Node
    pub async fn spawn_client(
        identifier: &'static str, port: u16, addr: IpAddr
    ) -> Res<Self> {
        todo!();
    }

}
