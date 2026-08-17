mod transmitter;

use std::env;
use transmitter::TransmitterProtocolHandler;

#[tokio::main]
async fn main() {
    let auth_token = env::var("AUTH_TOKEN").expect("AUTH_TOKEN must be set");
    let transmitter_id = env::var("TRANSMITTER_ID").expect("TRANSMITTER_ID must be set");
    println!("Starting transmitter: {transmitter_id}");
    let mut handler = TransmitterProtocolHandler::new(transmitter_id, auth_token);
    handler.start().await;
}
