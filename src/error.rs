#[derive(Clone, Debug)]
pub enum Error {
    FailedToEstablishTCPServer,
    FailedToEstablishUDPServer,
    FailedToEstablishTCPClient,
    FailedToEstablishUDPClient,
    DidNotReceiveUDPBroadcast,

    ChannelFailed,
    BroadcastFailed
}

pub type Res<T> = Result<T, Error>;
