use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;

use bytes::Bytes;

use crate::error::Error;
use crate::error::Res;

pub mod server;
pub mod client;

/// Push the referenced bytes onto a WriteHalf with a big endian size prefix
pub async fn send_bytes(write_half: &mut OwnedWriteHalf, bytes: &Bytes) -> Res<()> {
    // BE (big endian) representation of byte array size
    let be_len_repr = (bytes.len() as u32).to_be_bytes();

    // Write the length of the bytes followed by the bytes
    write_half.write_all(&be_len_repr).await.map_err(|_| Error::TcpChannelFailed)?;
    write_half.write_all(&bytes).await.map_err(|_| Error::TcpChannelFailed)?;
    Ok(())
}

// TODO End-Goal
// When a Client connects to the Server a packet
// containing the IP of every other client is sent
// Each message is 'signed' with the IP of the client who sent it
// (at the server end)
// Clients may give an addr to which the packet should be delivered
// This will be in the form of four bytes after the size bytes
// If the bytes are specified as 0,0,0,0 then the packet is broadcasted
