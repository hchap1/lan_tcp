#[derive(Clone, Debug)]
pub enum Error {
    FailedToEstablishTCPServer,
    FailedToEstablishUDPServer,
    FailedToEstablishTCPClient,
    FailedToEstablishUDPClient,
    DidNotReceiveUDPBroadcast,
    FailedToEstablishTCPConnection,
    CannotProcessIPV6,

    TcpChannelFailed,
    MpscChannelFailed,
    BroadcastFailed,
    UnableToAcquirePermit
}

pub type Res<T> = Result<T, Error>;
