use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;

use bytes::Bytes;

use crate::error::Error;
use crate::error::Res;

pub mod server;
pub mod client;

pub trait Headable {
    fn header(&self) -> Bytes;
    fn body(&self) -> &Bytes;
}

/// Push the referenced bytes onto a WriteHalf with a big endian size prefix
pub async fn send_bytes<T: Headable>(write_half: &mut OwnedWriteHalf, packet: &T) -> Res<()> {

    // Write the packet header followed by the body
    write_half.write_all(&packet.header()).await.map_err(|_| Error::TcpChannelFailed)?;
    write_half.write_all(packet.body()).await.map_err(|_| Error::TcpChannelFailed)?;
    Ok(())
}

// TODO End-Goal
// When a Client connects to the Server a packet
// containing the IP of every other client is sent
// Each message is 'signed' with the IP of the client who sent it
// (at the server end)
// Clients may give an addr to which the packet should be delivered
// This will be in the form of four bytes after the size bytes
// If the bytes are specified as 0 then the packet is broadcasted
// Otherwise, it's in the form N a1 b1 c1 d1 ... aN bN cN dN
// Where N is the number of IP addresses included

// The corresponding packets from Server -> Client should contain:
// sizebytes authoraddr (may be the server itself) data
