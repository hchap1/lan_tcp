use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;

use crate::error::Res;
use crate::error::Error;
use crate::networking::tcp::client;
use crate::networking::tcp::server;
use crate::networking::tcp::Headable;

#[derive(Clone, Debug)]
pub enum Destination {

    // Corresponds to 0 indicating all clients
    All,

    // Corresponds to 1abcd where IP of the server is a.b.c.d
    Server,

    // Corresponds to 10000 which will be interpreted as the server
    Single(Ipv4Addr),

    // Corresponds to Na1b1c1d1...aNbNcNdN
    // Where N is the number of IPs
    Multiple(Vec<Ipv4Addr>)
}

#[derive(Clone, Debug)]
pub struct SendPacket {
    pub data: Bytes,
    pub destination: Destination
}

#[derive(Clone, Debug)]
pub struct RecvPacket {
    pub data: Bytes,
    pub origination: Ipv4Addr
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
            Destination::Server => (1u8, vec![0u8; 4]),
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
    tcp_handle: Option<JoinHandle<Res<()>>>,

    // For servers only, keeps the UDP handler alive
    _udp_handle: Option<udp_discovery::server::Server>,

    // MPSC sender for handing bytes to be forwarded
    outgoing_queue: Sender<SendPacket>,

    // MPSC receiver for dequeuing incoming bytes
    pub incoming_queue: Receiver<RecvPacket>,

    // Semaphore, only for the server, to count clients
    semaphore: Option<(Arc<Semaphore>, usize)>
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
            Ok(server_addr) => match Self::spawn_client_from_information(
                identifier, port, server_addr
            ).await {
                
                // Ignore the first error and try again
                Err(_) => Self::spawn_client_from_information(identifier, port, server_addr).await,
                ok => ok
            },

            Err(e) => match e {

                // If nothing was received (a server doesn't exist)
                // Thus, attempt to start one of our own
                udp_discovery::error::Error::CouldNotFindServer => Self::spawn_server(
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
        identifier: &'static str,
        port: u16,
        max_connections: usize
    ) -> Res<Self> {

        // 1 Start TCP server task
        let (
            outgoing_queue,
            incoming_queue,
            tcp_handle,
            semaphore
        ) = server::construct_server(port, max_connections).await?;

        // 2 Start responding on UDP
        let _udp_handle = Some(
            udp_discovery::server::Server::spawn(identifier, port).await
        );

        // 3 Package handles and return
        Ok(Node {
            port,
            identifier,
            tcp_handle: Some(tcp_handle),
            _udp_handle,
            outgoing_queue,
            incoming_queue,
            semaphore: Some((semaphore, max_connections))
        })
    }

    /// After discovering a Server, build the recv and send threads
    /// Then package them together with MPSC into a Node
    pub async fn spawn_client_from_information(
        identifier: &'static str,
        port: u16,
        addr: IpAddr
    ) -> Res<Self> {

        // 1 Start TCP client task
        let (
            outgoing_queue,
            incoming_queue,
            tcp_handle
        ) = client::connect_client(addr, port).await?;

        // 2 Client does not use UDP after creation
        let _udp_handle = None;

        // 3 Package handles and return
        Ok(Node {
            port,
            identifier,
            tcp_handle: Some(tcp_handle),
            _udp_handle,
            outgoing_queue,
            incoming_queue,
            semaphore: None
        })
    }

    /// Attempt to discover a server over UDP
    // On failure, do not create a server
    pub async fn spawn_client(
        identifier: &'static str,
        port: u16,
    ) -> Res<Node> {

        // First, attempt to discover a Server via UDP broadcast
        match udp_discovery::client::discover(identifier, port).await {

            // If a server exists, then attempt to connect
            // If this fails, try again else report the critical failure
            Ok(server_addr) => match Self::spawn_client_from_information(
                identifier, port, server_addr
            ).await {
                
                // Ignore the first error and try again
                Err(_) => Self::spawn_client_from_information(identifier, port, server_addr).await,
                ok => ok
            },
            
            Err(_) => Err(Error::FailedToEstablishTCPClient)
        }
    }

    // Node helper methods

    /// Send a packet to the following destinations
    pub async fn send(&self, packet: Bytes, destination: Destination) -> Res<()> {
        self.outgoing_queue.send(SendPacket {
            data: packet,
            destination
        }).await.map_err(|_| Error::MpscChannelFailed)
    }

    /// Await the graceful termination of the node, by taking ownership of the handle
    pub async fn wait_for_close(&mut self) -> Res<()> {
        let handle = self.tcp_handle
            .take()
            .ok_or(Error::ThreadFailed)?;

        handle
            .await
            .map_err(|_| Error::ThreadFailed)?
    }

    // It is expected that the implementor uses the channel to receive messages

    /// Retrieve the semaphore, which exists only for servers
    pub fn get_semaphore(&self) -> Option<(Arc<Semaphore>, usize)> {
        self.semaphore.clone()
    }

    /// Wait for N connections by polling
    pub async fn await_n_clients(semaphore: Arc<Semaphore>, n: usize, max_connections: usize) {
        while max_connections - semaphore.available_permits() < n {
            tokio::time::sleep(tokio::time::Duration::from_secs(1));
        }
    }
}
