static FIRMWARE_VERSION: &str = "0.1.0-docker";
static MAX_RETRY_LIMIT_SECS: u64 = 60;
use std::time::{
    Instant,
};
use serde_json::json;
use tokio::{
    sync::mpsc,
    net::TcpStream,
};
use tokio_tungstenite::tungstenite::protocol::frame;
use tokio_tungstenite::{
    connect_async, 
    WebSocketStream,
    MaybeTlsStream,
    tungstenite::protocol::Message,
    tungstenite::client::IntoClientRequest,
    tungstenite::Error,
    tungstenite::protocol::CloseFrame
};
// use std::net::IPAddr;
use futures_util::{
    StreamExt, 
    SinkExt
};
use sysinfo::{System};

#[derive(serde::Deserialize, std::clone::Clone)]
struct Device {
    public_id: String,
    name: String,
    mac: String,
    ip: String
}

#[derive(serde::Deserialize, serde::Serialize)]
struct AuthMessage<'a> {
    r#type: &'a str,
    token: &'a str,
    hardware_id: &'a str,
    firmware_version: &'a str,
    ip: &'a str,
}

pub struct TransmitterProtocolHandler {
    time_started: std::time::Instant,
    auth_token: String,
    reconnect_count: u8,
    heartbeat_interval: u16,
    transmitter_id: String,
    pico_id: String,
    assigned_devices: Vec<Device>,
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
}

impl TransmitterProtocolHandler {
    pub fn new(transmitter_id: String, auth_token: String) -> Self {
        Self {
            time_started: std::time::Instant::now(),
            auth_token,
            reconnect_count: 0,
            heartbeat_interval: 15,
            transmitter_id,
            pico_id: String::new(),
            assigned_devices: vec![],
            heartbeat_handle: None,
        }
    }

    async fn handle_message(&mut self, message: serde_json::Value, send_channel: &mpsc::Sender::<Message>) {
        let send_json = async |value: serde_json::Value| {
            if let Err(e) = &send_channel.send(value.to_string().into()).await {
                eprintln!("Failed to send message: {}", e);
            };
        };
        if let Some(command_type) = message["type"].as_str() {
            println!("Message received, type: \"{}\"", &command_type);
            match command_type {
                "request_heartbeat" => {
                    println!("Sending message for requested heartbeat");    
                    let hearbeat: serde_json::Value = Self::get_sample_heartbeat(&self.time_started);
                    send_json(hearbeat).await;
                }
                "device_assignment" => {
                    if let Err(e) = self.update_devices(message["devices"].clone()){
                        println!("Update devices failed, incorrect json, Error: {}", e)
                    };
                }
                "wol" => {
                    let _ = self.handle_wol(message["mac"].to_string()).await;
                }
                "tcp_relay_open" => {

                }
                "tcp_relay_data" => {

                }
                "tcp_relay_close" => {

                }
                "ota_update" => {
                    send_json(json!({
                        "type": "ota_result",
                        "success": false,
                        "message": "Docker Transmitters cannot be updated remotely!"
                    })).await
                }
                "wifi_config_get" => {
                    send_json(json!({
                        "type": "wifi.config",
                        "pico_id": self.pico_id,
                        "networks": [],
                        "message": "Docker transmitters doesn't work on WiFi."
                        })
                    ).await
                }
                "wifi_config_set" => {
                    send_json(json!({
                        "type": "error",
                        "message": "Docker transmitters doesn't work on WiFi.",
                        })
                    ).await
                }
                "firmware_update_available" => {
                    //Do nothing and just log to std
                    println!("Doing nothing for firmware_update_available.");
                }
                "auth_ok" => {
                    println!("Authentication successful.");
                    self.pico_id = message["pico_id"].to_string();
                    self.assigned_devices = serde_json::from_value(message["assigned_devices"].clone()).unwrap_or(vec![]);
                    self.heartbeat_handle = Some(self.start_heartbeats(&send_channel).await);
                }

                "auth_fail" => {
                    println!("Authentication failed! Reason: {}", message["reason"])
                }

                "pong" => {
                    // Do nothing since this a ack reply to our heartbeat message
                }
                
                "identify" => {
                    //Do nothing since there's no LED to flash here.
                }
                _ => {
                    println!("Received unknown command type: {}", command_type);
                } 
            } 
        } 
        else {
            println!("Missing message type");
        }
    }       

    async fn handle_wol(&self, to_wake_mac: String) -> Result<(), String> {
        // let request_id: String = message["request_id"].to_string();

        if !self.assigned_devices.iter().any(|device: &Device| device.mac == to_wake_mac) {
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

    async fn start_heartbeats(&self, send_channel: &mpsc::Sender<Message>) -> tokio::task::JoinHandle<()> {
        let send_channel: mpsc::Sender<Message> = send_channel.clone();
        let heartbeat_interval: u16 = self.heartbeat_interval;
        let start_time: std::time::Instant = self.time_started.clone();
        tokio::spawn(async move {
            println!("Heartbeat task started, sending every {} seconds.", &heartbeat_interval);
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(heartbeat_interval as u64)).await;
                if let Err(e) = send_channel.send(Self::get_sample_heartbeat(&start_time).to_string().into()).await {
                    eprintln!("Failed to send heartbeat: {}", e);
                    break;
                }
            }
        })
    }

    fn get_sample_heartbeat(started_at: &Instant) -> serde_json::Value {
        let sys: System = System::new_all();
        let now: Instant = Instant::now();
        json!({
            "type": "heartbeat",
            "health": {
                "free_ram": sys.total_memory() - sys.used_memory(),
                "total_ram": sys.total_memory(),
                "wifi_rssi": -50,
                "uptime_seconds": now.duration_since(*started_at).as_secs(),
                "reconnect_count": 0,
                "flash_free": 512,
                "flash_total": 1024
            }
        })
    }


    async fn open_websocket(&self) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>
                                                                >,
                                                                Error> {
        let device_url =  format!("wss://wakemypc.com/ws/pico/{}/", &self.transmitter_id);
        println!("Connecting to server at URL: {}", &device_url);

        let mut request = device_url.into_client_request().unwrap();
        request.headers_mut()
            .insert(
                "User-Agent", 
                "wakemypc-rust-transmitter/0.1.0".parse().unwrap()
            );
        let (ws_stream, _response) = connect_async(request.clone()).await?;
        Ok(ws_stream)
    }

    fn get_authentication_message(&self) -> Message {
        // Create the authentication message
        let auth_message: AuthMessage = AuthMessage { 
                r#type: "auth",
                token: &self.auth_token,
                hardware_id: &self.transmitter_id,
                firmware_version: FIRMWARE_VERSION,
                ip: &local_ip_address::local_ip().unwrap().to_string()
        };
        Message::Text(serde_json::to_string(&auth_message).unwrap().into())
    }

    pub async fn start(&mut self) {
        let mut retry_time: u64 = 1;
        let mut wait_and_backoff = async |reset| {
            if reset {
                retry_time = 1;
                return;
            };
            println!("Waiting {} secs...", retry_time);
            tokio::time::sleep(tokio::time::Duration::from_secs(retry_time)).await;
            retry_time *= 2;
            retry_time = {
                if retry_time > MAX_RETRY_LIMIT_SECS{
                    MAX_RETRY_LIMIT_SECS
                } else {
                    retry_time
                }
            }
        };
        let mut stop_now: bool = false;
        // if sigterm is received, we break out of the loop and exit the program.
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate()
            ).unwrap();
        loop {
            // Retry until auth is succesful
            match self.open_websocket().await {
                Ok(ws_stream) => {
                    let (mut write_half, mut read_half) = ws_stream.split();

                    // Create a multi producter and single-consumer asynchronous channel
                    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Message>(32);
                    let (incoming_tx, mut incoming_rx) = mpsc::channel::<serde_json::Value>(32);
                    
                    let outgoing_tx_clone = outgoing_tx.clone();

                    // Write task: drains the channel and forwards messages to the WebSocket
                    let mut write_task = tokio::spawn(async move {
                        while let Some(msg) = outgoing_rx.recv().await {
                            if let Err(e) = write_half.send(msg).await {
                                eprintln!("Write error: {}", e);
                                break;
                            }
                        }
                    });

                    // Read task: handles incoming messages; routes pongs back through the channel
                    let mut read_task = tokio::spawn(async move {
                        while let Some(result) = read_half.next().await {
                            match result {
                                Ok(Message::Text(text)) => {
                                    match serde_json::from_str::<serde_json::Value>(text.as_str()) {
                                        Ok(json_message) => {
                                            if let Err(e) = incoming_tx.send(json_message).await {
                                                eprintln!("Failed to receive incoming message: {}", e);
                                                break;
                                            }
                                        }

                                        Err(e) => {
                                            eprintln!("Invalid JSON: {}", e);
                                        }
                                    }
                                }

                                Ok(Message::Ping(payload)) => {
                                    outgoing_tx_clone
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
                                    // return Err(e);
                                    break;
                                }

                                _ => {}
                            }
                        }
                    });
                    // begin receiving and sending messages,
                    if let Err(e) = outgoing_tx.send(self.get_authentication_message()).await {
                        println!("{}", e);
                        wait_and_backoff(false).await;
                        continue;
                    }
                    wait_and_backoff(true).await;
                    loop {
                        tokio::select! {
                            //sigterm is received, we break out of the loop and exit the program.
                            _ = sigterm.recv() => {
                                // println!("SIGTERM received, shutting down.");
                                // send a close frame to the server for closing the connection
                                _ = outgoing_tx.send(Message::Close(Some(CloseFrame {
                                    code: frame::coding::CloseCode::Normal,
                                    reason: "Shutting down.".into(),
                                }))).await;
                                write_task.abort();
                                read_task.abort();
                                stop_now = true;
                                break;
                            }
                            Some(json_message) = incoming_rx.recv() => {
                                match json_message["type"].as_str() {
                                    // if the server sends a reboot command, we abort the read and write tasks and break out of the loop to reconnect.
                                    Some("reboot") => {
                                        println!("Rebooting transmitter as requested by server.");
                                        // send a close frame to the server for closing the connection
                                        _ = outgoing_tx.send(Message::Close(Some(CloseFrame {
                                            code: frame::coding::CloseCode::Normal,
                                            reason: "Rebooting".into(),
                                        }))).await;
                                        read_task.abort();
                                        write_task.abort();
                                        break;
                                    }
                                    _ => {
                                        self.handle_message(json_message, &outgoing_tx).await;
                                    }
                                }
                            },
                            // break exists so that when either of them returns(something happened such that they returned) 
                            // we can reconnect to the websocket.
                            _ = &mut read_task => {
                                // If the read task ends, we abort the write task and break to reconnect
                                write_task.abort();
                                break;
                            },
                            _ = &mut write_task => {
                                // If the write task ends, we abort the read task and break to reconnect
                                read_task.abort();
                                break;
                            }
                        }
                    }
                    // If we reach here, it means either the read or write task has ended, so we abort the heartbeat task if it exists.
                    if let Some(handle) = &self.heartbeat_handle.take(){
                        handle.abort();
                    }
                }
                Err(e) => {
                    println!("Error while opening websocket: {}", e)
                }
            } 
            //if we receive a sigterm, we break out of the loop and exit the program.
            if stop_now {
                println!("Received Shutdown signal, exiting.");
                break;
            }
            // if the websocket closes begin the auth process again,
            wait_and_backoff(false).await;
        }

    }

}
