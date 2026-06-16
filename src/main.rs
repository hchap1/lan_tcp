pub mod error;
pub mod networking;

use bytes::Bytes;

use crate::networking::node::{Destination, Node};
use crate::error::Res;

#[tokio::main]
async fn main() -> Res<()> {

    println!("If I were server, I would use {:?}", udp_discovery::server::Server::find_suitable_ipv4().await);

    let mut node: Node = Node::spawn("something-unique", 12345, 100).await?;
    
    match node.get_semaphore() {
        Some((server_semaphore, max_connections)) => {
            Node::await_n_clients(server_semaphore, 1, max_connections).await;
            node.send(Bytes::from_static(&[1, 2, 3, 4, 5]), Destination::All).await?;
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }

        None => {
            match node.incoming_queue.recv().await {
                Some(packet) => println!("{} bytes received from {:?}", packet.data.len(), packet.origination),
                None => println!("Channel dropped before a message was received")
            }
        }
    }

    Ok(())

}
