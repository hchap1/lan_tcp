# Distributed Node Networking

A lightweight peer-to-peer networking library for Rust that automatically discovers peers on the local network and elects a server when one does not already exist.

The library combines:

- **UDP service discovery** for finding existing servers
- **TCP communication** for reliable packet transport
- **Automatic server election** when no server is found
- **Client/server abstraction** through a single `Node` API
- **Asynchronous Tokio-based networking**
- **Broadcast and targeted messaging**

## Features

- Zero-configuration local network discovery
- Automatic server creation if none exists
- Send packets to:
  - All connected clients
  - The server
  - A single client
  - Multiple clients
- Tokio-based async API
- Efficient packet storage using `bytes::Bytes`
- Connection counting via semaphores
- Simple high-level API

---

# Installation

```toml
[dependencies]
your-crate-name = "0.1"
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

---

# Quick Start

The same code can be run on every machine.

The first node started will become the server.

Subsequent nodes will automatically discover and connect to it.

```rust
use bytes::Bytes;

use your_crate_name::networking::node::{
    Node,
    Destination
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut node = Node::spawn(
        "my-application",
        12345,
        100
    ).await?;

    match node.get_semaphore() {

        // We are the server
        Some((server_semaphore, max_connections)) => {

            println!("Waiting for a client...");

            Node::await_n_clients(
                server_semaphore,
                1,
                max_connections
            ).await;

            node.send(
                Bytes::from_static(b"Hello world!"),
                Destination::All
            ).await?;

            println!("Message sent!");
        }

        // We are a client
        None => {

            match node.incoming_queue.recv().await {

                Some(packet) => {
                    println!(
                        "Received {} bytes from {:?}",
                        packet.data.len(),
                        packet.origination
                    );
                }

                None => {
                    println!("Connection closed");
                }
            }
        }
    }

    Ok(())
}
```

---

# How It Works

When a node starts:

1. A UDP broadcast is sent across the local network.
2. If a server responds:
   - The node becomes a client.
   - A TCP connection is established.
3. If no server responds:
   - The node becomes the server.
   - A TCP listener is created.
   - UDP discovery responses begin.

This allows applications to form a network automatically without requiring users to manually enter IP addresses.

---

# Creating a Node

```rust
let node = Node::spawn(
    "my-app",
    12345,
    100
).await?;
```

Parameters:

| Parameter | Description |
|------------|-------------|
| `identifier` | Unique application identifier used during UDP discovery |
| `port` | TCP and UDP port |
| `max_connections` | Maximum server connections |

The identifier prevents unrelated applications on the same network from connecting to one another.

---

# Sending Data

Packets are sent using `bytes::Bytes`.

```rust
node.send(
    Bytes::from_static(&[1, 2, 3]),
    Destination::All
).await?;
```

---

# Destinations

## Broadcast

Send to every connected client.

```rust
Destination::All
```

## Server

Send to the server.

```rust
Destination::Server
```

## Single Client

Send to a specific IPv4 address.

```rust
Destination::Single(
    "192.168.1.10".parse()?
)
```

## Multiple Clients

Send to several clients.

```rust
Destination::Multiple(vec![
    "192.168.1.10".parse()?,
    "192.168.1.11".parse()?
])
```

---

# Receiving Data

Incoming packets are delivered through an async channel.

```rust
while let Some(packet) =
    node.incoming_queue.recv().await
{
    println!(
        "{} bytes from {}",
        packet.data.len(),
        packet.origination
    );
}
```

The packet contains:

```rust
pub struct RecvPacket {
    pub data: Bytes,
    pub origination: Ipv4Addr
}
```

---

# Detecting Server vs Client

A node becomes either a server or client automatically.

You can determine which role was assigned:

```rust
match node.get_semaphore() {

    Some(_) => {
        println!("I am the server");
    }

    None => {
        println!("I am a client");
    }
}
```

---

# Waiting for Clients

Servers can wait until a desired number of clients have connected.

```rust
Node::await_n_clients(
    semaphore,
    5,
    max_connections
).await;
```

This is useful for multiplayer games, collaborative applications, or synchronized startup sequences.

---

# Graceful Shutdown

Wait for the networking task to terminate:

```rust
node.wait_for_close().await?;
```

---

# Architecture

```text
                UDP Discovery
         ┌─────────────────────┐
         │  Broadcast Probe    │
         └──────────┬──────────┘
                    │
          Server Found?
            │       │
          Yes       No
            │       │
            ▼       ▼
       TCP Client  TCP Server
            │       │
            └───┬───┘
                │
           Node API
                │
        Send / Receive Bytes
```

---

# Typical Use Cases

- Multiplayer games
- Local network tools
- Robotics communication
- Distributed simulations
- Collaborative applications
- Device discovery systems
- LAN messaging

---

# Notes

- Discovery currently operates on local IPv4 networks.
- TCP is used for all payload transmission.
- UDP is used only for discovery.
- The first node started becomes the server.
- Subsequent nodes automatically connect to it.

---

# License

Licensed under either:

- MIT License
- Apache License 2.0

at your option.
