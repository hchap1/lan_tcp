use crate::networking::interface::Interface;
use udp_discovery::client::discover;

pub struct Client {
    
}

impl Interface for Client {
    
    async fn establish(identifier: &'static str, port: u16) -> crate::error::Res<Self>
        where Self: Sized {
        
        match discover(identifier, port).await {
            Ok(ip_addr) => {

            }
        }
    }
}
