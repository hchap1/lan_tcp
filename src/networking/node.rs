use std::net::IpAddr;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;

use crate::error::Res;

pub struct Node {

    // Port and code designated for this application
    port: u16,
    identifier: &'static str,

    // Thread listening for incoming TCP packets for acting Clients
    // For acting Servers, this instead handles both simultaneously
    recv_handle: JoinHandle<Res<()>>,

    // Thread responsible for processing outgoing messages for Clients
    // For acting Servers, this instead handles UDB broadcast handling
    send_handle: JoinHandle<Res<()>>,

    // MPSC sender for handing bytes to be forwarded
    outgoing_queue: Sender<Bytes>,

    // MPSC receiver for dequeuing incoming messages
    // Note that this is functionally identical for server/client
    incoming_queue: Receiver<Bytes>
}

impl Node {

    /// Construct the threads and callback structure for a Server
    /// Then package them together with UDP advertisement into a Node
    pub async fn spawn_server(
        identifier: &'static str, port: u16
    ) -> Self {
        // 1 Start TCP server & threads
        // 2 Start responding on UDP
        // 3 Package handles and return
        todo!();
    }

    /// After discovering a Server, build the recv and send threads
    /// Then package them together with MPSC into a Node
    pub async fn spawn_client(
        identifier: &'static str, port: u16, addr: IpAddr
    ) -> Res<Self> {
        todo!();
    }

}
