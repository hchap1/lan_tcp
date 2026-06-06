use std::net::IpAddr;
use std::net::Ipv4Addr;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;

use crate::error::Res;
use crate::error::Error;
use crate::networking::tcp::client;
use crate::networking::tcp::server;
use crate::networking::tcp::Headable;

pub enum Destination {

    // Corresponds to 0 indicating all clients
    All,

    // Corresponds to 1abcd where IP is a.b.c.d
    Single(Ipv4Addr),

    // Corresponds to Na1b1c1d1...aNbNcNdN
    // Where N is the number of IPs
    Multiple(Vec<Ipv4Addr>)
}

pub struct SendPacket {
    data: Bytes,
    destination: Destination
}

pub struct RecvPacket {
    data: Bytes,
    origination: Ipv4Addr
}

impl Headable for SendPacket {
    /// 4 byte representation of size
    /// 1 byte representing destination count n
    /// 4n byte representation of destinations
    fn header(&self) -> Bytes {
        let size_header = (self.data.len() as u32).to_be_bytes();
        let mut byte_vec = Vec::from(size_header);

        // Construct the byte representation of the addressing
        let (count, mut bytes) = match &self.destination {
            Destination::All => (0u8, Vec::new()),
            Destination::Single(addr) => (1u8, Vec::from(addr.octets())),
            Destination::Multiple(addrs) => (addrs.len() as u8, addrs
                .into_iter()
                .map(|a| a.octets())
                .flatten()
                .collect()
            )
        };

        byte_vec.push(count);
        byte_vec.append(&mut bytes);

        Bytes::from(bytes)
    }

    fn body(&self) -> &Bytes {
        &self.data
    }
}

impl Headable for RecvPacket {
    /// 4 byte representation of size
    /// 4 byte representation of author
    fn header(&self) -> Bytes {
        let mut vec = Vec::with_capacity(8);
        vec.extend_from_slice(&(self.data.len() as u32).to_be_bytes());
        vec.extend_from_slice(&self.origination.octets());
        Bytes::from(vec)
    }

    fn body(&self) -> &Bytes {
        &self.data
    }
}

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

    // Node helper methods
    pub async fn send(packet: Bytes, desination: Destination) -> Res<()> {

    }
}
