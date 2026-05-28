use std::fmt::Debug;
use crate::error::Res;

pub trait Serial {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Self;
}

pub trait P:
    Clone + Debug + Serial
{}

impl<T> P for T
where T: Clone + Debug + Serial
{}

#[derive(Clone, Debug)]
pub enum Message {
    Data(Vec<u8>),
    Close,
    ServerTerminatingWaitForNewServer,
    ServerTerminatingBecomeNewServer,
}

#[derive(Clone, Debug)]
pub enum Status {
    NoConnections,
    ChannelDead,
    ChannelAlive
}

pub trait Interface<T: P> {

    /// Create the interface
    async fn establish(identifier: &'static str, port: u16) -> Res<Self>
    where Self: Sized;

    /// Retrieve the status of the connection
    /// Used to distinguish when a client should 'give up' and become a server
    fn status(&self) -> Status;

    /// Generic 'broadcast' to every other listener
    async fn broadcast(&self, packet: T) -> Res<()> {
        self.send(Message::Data(
            packet.to_bytes()
        )).await
    }

    /// Send a message to the Server
    async fn send(&self, message: Message) -> Res<()>;

    /// Retrieve the first bytes in the queue or wait for it to be populated
    async fn receive_bytes(&self) -> Res<Vec<u8>>;
    
    // Retrieve the first message in the queue or wait for it to be populated
    async fn receive(&self) -> Res<T> {
        Ok(T::from_bytes(self.receive_bytes().await?))
    }

    /// Check if the queue is non-empty. false means there is nothing in the queue.
    fn peek_queue(&self) -> bool;
    
    /// [Attempt to] gracefully shut down the connection
    /// Yields any messages still in the queue
    async fn terminate(&self) -> Res<Vec<T>> {
        self.send(Message::Close).await?;
        let mut queue = vec![];
        while self.peek_queue() {
            queue.push(self.receive().await?)
        }
        Ok(queue)
    }
}
