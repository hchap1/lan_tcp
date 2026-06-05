#[derive(Clone, Debug)]
pub enum Error {
    FailedToEstablishTCPServer,
    FailedToEstablishUDPServer,
    FailedToEstablishTCPClient,
    FailedToEstablishUDPClient,
    DidNotReceiveUDPBroadcast,
    FailedToEstablishTCPConnection,

    ChannelFailed,
    BroadcastFailed,
    UnableToAcquirePermit
}

pub type Res<T> = Result<T, Error>;
