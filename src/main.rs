use std::env;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async, 
    tungstenite::protocol::Message,
    tungstenite::client::IntoClientRequest
};
use serde::Deserialize;
// use std::net::IPAddr;
use futures_util::{
    StreamExt, 
    SinkExt
};
use sysinfo::{System};
use local_ip_address::{local_ip};

#[derive(serde::Deserialize)]
struct Device {
    public_id: String,
    name: String,
    mac: String,
    ip: String
}

struct TransmitterProtocolHandler {
    heartbeat_interval: u32,
    _transmitter_id: String,
    pico_id: String,
    send_channel: mpsc::Sender<Message>,
    assigned_devices: Vec<Device>
}

impl TransmitterProtocolHandler {
    fn new(_transmitter_id: String, send_channel: &mpsc::Sender<Message>) -> Self {
        Self {
            heartbeat_interval: 15,
            _transmitter_id,
            pico_id: String::new(),
            send_channel: send_channel.clone(),
            assigned_devices: vec![]
        }
    }

    async fn handle_message(&mut self, message: serde_json::Value) {
        let command_type = &message["type"];
        match command_type.as_str() {
            Some("request_heartbeat") => {
                self.send_heartbeat().await;
            }
            Some("device_assignment") => {
                let _ = self.update_devices(message["devices"].clone());
            }
            Some("wol") => {
                self.handle_wol(message["mac"].to_string()).await;
            }
            Some("ota_update") => {
                self.send_json(json!({
                    "type": "ota_result",
                    "success": false,
                    "message": "Docker Transmitters cannot be updated remotely!"
                })).await
            }
            Some("firmware_update_available") => {
                //Do nothing and just log to std
                println!("Recieved Firmware update available.");
            }
            Some("pong") => {
                // Do nothing since this a ack reply to our heartbeat message
            }
            _ => {
                println!("Received unknown command type: {}", &command_type);
            }
        }
    }

    async fn handle_wol(&self, to_wake_mac: String) -> Result<(), String> {
        // let request_id: String = message["request_id"].to_string();

        if !self.assigned_devices.iter().any(|device| device.mac == to_wake_mac) {
            Err(String::from("Mac Address not assigned. Prevented Wake-on-lan Packet from being sent"))
        }
        else {
            println!("Wake on lan packet sent");
            Ok(())
        }
        
    }

    fn update_devices(&mut self, devices: serde_json::Value) -> Result<(), serde_json::Error>  {
        self.assigned_devices = serde_json::from_value(devices)?;
        Ok(())
    }

    async fn start_heartbeats(&self) {
        let send_channel = self.send_channel.clone();
        let heartbeat_interval = self.heartbeat_interval;
        
        tokio::spawn(async move {
            println!("Heartbeat task started, sending every {} seconds.", &heartbeat_interval);
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(heartbeat_interval as u64)).await;
                if let Err(e) = send_channel.send(Self::get_sample_heartbeat().to_string().into()).await {
                    eprintln!("Failed to send heartbeat: {}", e);
                    break;
                }
            }
        });
    }

    async fn send_json(&self, message: serde_json::Value) {
        if let Err(e) = self.send_channel.send(message.to_string().into()).await {
            eprintln!("Failed to send message: {}", e);
        }
    }

    fn get_sample_heartbeat() -> serde_json::Value {
        let sys = System::new_all();
        json!({
            "type": "heartbeat",
            "health": {
                "free_ram": sys.total_memory() - sys.used_memory(),
                "total_ram": sys.total_memory(),
                "wifi_rssi": -50,
                "uptime_seconds": 0,
                "reconnect_count": 0,
                "flash_free": 512,
                "flash_total": 1024
            }
        })
    }

    async fn send_heartbeat(&self) {
        println!("Sending message for requested heartbeat");
        self.send_json(Self::get_sample_heartbeat()).await;
    }

}


#[tokio::main]
async fn main() {
    let _auth_token = env::var("AUTH_TOKEN").expect("AUTH_TOKEN must be set");
    let server_url = env::var("SERVER_URL").unwrap_or_else(|_| "wss://wakemypc.com".to_string());

    let transmitter_id = env::var("TRANSMITTER_ID").expect("TRANSMITTER_ID must be set");
    println!("Starting transmitter: {transmitter_id}");
    
    let device_url = { server_url.clone() + "/ws/pico/" + &transmitter_id + "/"};
    println!("Connecting to server at URL: {device_url}");

    let mut request = device_url.into_client_request().unwrap();
    request.headers_mut().insert("User-Agent", "wakemypc-rust-transmitter/0.1.0".parse().unwrap());

    // Create the authentication message
    let auth_message = Message::Text(json!({
            "type": "auth",
            "token": _auth_token,
            "hardware_id": transmitter_id.clone(),
            "firmware_version": "0.1.0",
            "ip": local_ip().unwrap().to_string(), // TODO: Replace with actual IP address retrieval logic
        }).to_string().into()
    );

    // Connect to WebSocket server
    let (ws_stream, _response) = connect_async(request).await.expect("Failed to connect to WebSocket server");
    println!("WebSocket connection established, ready to send/receive messages.");

    // This allows you to read and write concurrently without borrowing errors.
    let (mut write_half, mut read_half) = ws_stream.split();

    // Create a multi producter and single-consumer asynchronous channel
    let (tx, mut rx) = mpsc::channel::<Message>(32);
    let tx_clone = tx.clone();

    // Write task: drains the channel and forwards messages to the WebSocket
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = write_half.send(msg).await {
                eprintln!("Write error: {}", e);
                break;
            }
        }
    });
    // Read task: handles incoming messages; routes pongs back through the channel
    let read_task = tokio::spawn(async move {
        //Initialize the handler as None, it will be set when we receive the first message with a public_id
        let mut handler = TransmitterProtocolHandler::new(
                                        transmitter_id.clone(),
                                        &tx_clone
        );
        while let Some(result) = read_half.next().await {
            match result {
                Ok(Message::Text(text)) => {
                    match serde_json::from_str::<serde_json::Value>(&text) {
                        Ok(json_message) => {
                            match json_message["type"].as_str() {
                                Some("auth_ok") => {
                                    println!("Authentication successful.");
                                    handler.pico_id = json_message["pico_id"].to_string();
                                    let _ = handler.update_devices(json_message["assigned_devices"].clone()).unwrap();
                                    handler.start_heartbeats().await;
                                }

                                Some("auth_fail") => {
                                    println!("Authentication failed! Reason: {}", &json_message["reason"])
                                }

                                Some(_) => {
                                    println!("Received message: {}", &json_message);
                                    handler.handle_message(json_message).await;
                                }

                                None => {
                                    println!("Missing message type");
                                }
                            }
                        }

                        Err(e) => {
                            eprintln!("Invalid JSON: {}", e);
                        }
                    }
                }

                Ok(Message::Ping(payload)) => {
                    tx_clone
                        .send(Message::Pong(payload))
                        .await
                        .unwrap();
                }

                Ok(Message::Close(_)) => {
                    println!("Websocket Connection Closed.");
                    break;
                }

                Err(e) => {
                    eprintln!("Read error {}", e);
                    break;
                }

                _ => {}
            }
        }
    });

    // Send the authentication message through the channel (write_half is owned by write_task)
    if let Err(e) = tx.send(auth_message).await {
        eprintln!("Error sending auth message: {}", e);
        return;
    }
    println!("Authentication message sent.");
    // test heartbeat message
    tokio::select! {
        _ = write_task => {}
        _ = read_task => {}
    }
}
