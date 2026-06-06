use std::net::IpAddr;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;

use crate::error::Res;
use crate::error::Error;
use crate::networking::tcp::client;
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

    /// Attempt to find an acting Server via UDP broadcast
    /// If this fails, then instead attempt to become the server
    pub async fn spawn(
        identifier: &'static str, port: u16, max_connections: usize
    ) -> Res<Self> {
        
        // First, attempt to discover a Server via UDP broadcast
        match udp_discovery::client::discover(identifier, port).await {

            // If a server exists, then attempt to connect
            // If this fails, try again else report the critical failure
            Ok(server_addr) => match Self::spawn_client(
                identifier, port, server_addr
            ).await {
                
                // Ignore the first error and try again
                Err(_) => Self::spawn_client(identifier, port, server_addr).await,
                ok => ok
            },

            Err(e) => match e {

                // If nothing was received (a server doesn't exist)
                // Thus, attempt to start one of our own
                udp_discovery::error::Error::RecvFailed => Self::spawn_server(
                    identifier, port, max_connections
                ).await,

                // A 'server' was contacted but failed the security challenge
                udp_discovery::error::Error::InvalidIdentifier => {
                    eprintln!("Probed Server, but failed security challenge.");
                    Err(Error::FailedToEstablishTCPConnection)
                },

                // All other UDP related errors associate with being unable to
                // listen, due to some failure of the UDP client
                _ => Err(Error::FailedToEstablishUDPClient)
            },
        }

    }

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

        // 1 Start TCP client task
        let (
            outgoing_queue,
            incoming_queue,
            tcp_handle
        ) = client::connect_client(addr, port).await?;

        // 2 Client does not use UDP after creation
        let udp_handle = None;

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

}
