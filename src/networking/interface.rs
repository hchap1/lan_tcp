use serde::{Serialize, DeseralizeOwned};
use crate::error::Res;

pub trait P:
    Clone + Debug + Serialize + DeserializeOwned
{}

impl<T> P for T
where T: Clone + Debug + Serialize + DeserializeOwned
{}

#[derive(Clone, Debug)]
pub enum Message<T: P> {
    Data(T),
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
    async fn establish() -> Res<Self>
    where Self: Sized;

    /// Retrieve the status of the connection
    /// Used to distinguish when a client should 'give up' and become a server
    fn status(&self) -> Status;

    /// Generic 'broadcast' to every other listener
    async fn broadcast(&self, packet: T) -> Res<()> {
        self.send(Message::Data(packet))
    }

    /// Send a message to the Server
    async fn send(&self, message: Message<T>) -> Res<()>;

    /// Retrieve the first message in the queue or wait for it to be populated
    async fn receive(&self) -> Res<T>;
    
    /// [Attempt to] gracefully shut down the connection
    /// Yields any messages still in the queue
    async fn terminate(&self) -> Res<Vec<T>> {
        self.send(Message::Close)
    }
}
