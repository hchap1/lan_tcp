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
    write_half.write_all(&be_len_repr).await.map_err(|_| Error::ChannelFailed)?;
    write_half.write_all(&bytes).await.map_err(|_| Error::ChannelFailed)?;
    Ok(())
}

