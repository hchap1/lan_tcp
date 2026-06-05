#[derive(Clone, Debug)]
pub enum Error {
    FailedToEstablishTCPServer,
    FailedToEstablishUDPServer,
    FailedToEstablishTCPClient,
    FailedToEstablishUDPClient,
    DidNotReceiveUDPBroadcast,
}

pub type Res<T> = Result<T, Error>;
