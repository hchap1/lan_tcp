// Channel size
pub const CHANNEL_SIZE: usize = 1024;

/// Publicly exposed interface
pub mod node;

/// Leverage github.com/hchap1/udp_discovery to do one of:
/// - Discover a server via UDP and return address information
/// - Fail to discover a server and start one of its own
/// - Didn't receive anything and failed to start server
mod udp;

/// Asynchronous TCP handler
/// Seperated into distinct Server/Client constructors
/// This builds the threads and inter-thread communication
/// (via MPSC), of which the receivers/producers are exposed to Node later
mod tcp;
