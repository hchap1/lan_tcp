use std::net::IpAddr;

use tokio::task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;

use crate::error::Res;

pub struct Node {

    // Port and code designated for this application
    port: u16,
    identifier: &'static str,

    // Thread listening for incoming TCP packets
    recv_handle: JoinHandle<Res<()>>,

    // Thread responsible for processing outgoing messages
    send_handle: JoinHandle<Res<()>>,

    /// Owns TCP connections for acting Servers only
    clnt_handle: Option<JoinHandle<Res<()>>>,

    // MPSC sender for handing messages to the send thread
    outgoing_queue: Sender<Vec<u8>>,

    // MPSC receiver for dequeuing incoming messages
    // Note that this is functionally identical for server/client
    // None signals shutdown
    incoming_queue: Receiver<Option<Vec<u8>>>
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
