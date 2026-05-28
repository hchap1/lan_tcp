#[derive(Clone, Debug)]
pub enum Error {
    FailedToEstablishServer,
    FailedToEstablishClient,
    DidNotReceiveUDPBroadcast,
}

pub type Res<T> = Result<T, Error>;
