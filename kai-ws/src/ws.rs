use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use bytes::Bytes;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt, stream::{SplitSink, SplitStream}};
use velocityx::ConcurrentHashMap;

use crate::{Error, error::Result};

pub const TYPE_RESPONSE: u32 = 0x00000000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage {
    pub sn: u32,
    pub payload_type: u32,
    pub payload: serde_json::Value,
}

pub struct WsSender {
    pub split_sink: SplitSink<WebSocketStream<TcpStream>, Message>,
}

impl WsSender {
    pub async fn send_json(&mut self, msg: impl Serialize) -> Result<()> {
        let json = serde_json::to_string(&msg)?;
        self.split_sink.send(Message::text(json)).await?;
        Ok(())
    }

    pub async fn send_bin(&mut self, data: impl Into<Bytes>) -> Result<()> {
        self.split_sink.send(Message::binary(data)).await?;
        Ok(())
    }
}

type WsSenderLock = Arc<Mutex<WsSender>>;

#[async_trait]
pub trait WsBinaryProcessor: Send + Sync + 'static {
    async fn process_bin(&self, data: &[u8], sender: &mut WsSenderLock) -> Result<()>;
}

#[async_trait]
pub trait WsJsonProcessor: Send + Sync + 'static {
    async fn process_json(&self, data: serde_json::Value, sender: &mut WsSenderLock) -> Result<()>;
}

pub const OFFSET_SN: usize = 0;
pub const LEN_SN: usize = 4;

pub const OFFSET_PAYLOAD_TYPE: usize = OFFSET_SN + LEN_SN;
pub const LEN_PAYLOAD_TYPE: usize = 4;

pub fn parse_bin_sn(data: &[u8]) -> Result<u32> {
    let sn_bin: [u8; 4] = data[OFFSET_SN..OFFSET_SN + LEN_SN].try_into()?;
    Ok(u32::from_be_bytes(sn_bin))
}

pub fn parse_bin_payload_type(data: &[u8]) -> Result<u32> {
    let type_bin: [u8; 4] = data[OFFSET_PAYLOAD_TYPE..OFFSET_PAYLOAD_TYPE + LEN_PAYLOAD_TYPE].try_into()?;
    Ok(u32::from_be_bytes(type_bin))
}

pub struct WsMessageDispatcher {
    sender: WsSenderLock,
    receiver: SplitStream<WebSocketStream<TcpStream>>,
    request_bin_processor_map: ConcurrentHashMap<u32, Arc<dyn WsBinaryProcessor>>,
    request_json_processor_map: ConcurrentHashMap<u32, Arc<dyn WsJsonProcessor>>,
    reponse_bin_processor_map: ConcurrentHashMap<u32, Arc<dyn WsBinaryProcessor>>,
    reponse_json_processor_map: ConcurrentHashMap<u32, Arc<dyn WsJsonProcessor>>,
}

impl WsMessageDispatcher {
    pub fn new(sender: WsSenderLock, receiver: SplitStream<WebSocketStream<TcpStream>>) -> Self {
        Self {
            sender,
            receiver,
            request_bin_processor_map: ConcurrentHashMap::new(),
            request_json_processor_map: ConcurrentHashMap::new(),
            reponse_bin_processor_map: ConcurrentHashMap::new(),
            reponse_json_processor_map: ConcurrentHashMap::new(),
        }
    }

    pub fn set_bin_processor(&self, payload_type: u32, processor: Arc<dyn WsBinaryProcessor>) {
        self.request_bin_processor_map.insert(payload_type, processor);
    }

    pub fn set_json_processor(&self, payload_type: u32, processor: Arc<dyn WsJsonProcessor>) {
        self.request_json_processor_map.insert(payload_type, processor);
    }

    pub async fn process(&mut self) -> Option<Result<()>> {
        let mut result = None;
        if let Some(msg) = self.receiver.next().await {
            match msg {
                Ok(msg) => {
                    match msg {
                        Message::Text(json) => {
                            match serde_json::from_str::<WsMessage>(&json) {
                                Ok(message) => {
                                    let processor = if message.payload_type == TYPE_RESPONSE {
                                        self.reponse_json_processor_map.get(&message.sn)
                                    } else {
                                        self.request_json_processor_map.get(&message.payload_type)
                                    };
                                    if let Some(processor) = processor {
                                        let r = processor.process_json(message.payload, &mut self.sender).await;
                                        result = Some(r);
                                    }
                                },
                                Err(e) => {
                                    result = Some(Err(Error::from(e)));
                                }
                            };
                        }
                        Message::Binary(data) => {
                            match parse_bin_sn(data.as_ref()) {
                                Ok(sn) => {
                                    match parse_bin_payload_type(data.as_ref()) {
                                        Ok(payload_type) => {
                                            let processor = if payload_type == TYPE_RESPONSE {
                                                self.reponse_bin_processor_map.get(&sn)
                                            } else {
                                                self.request_bin_processor_map.get(&payload_type)
                                            };
                                            if let Some(processor) = processor {
                                                let r = processor.process_bin(data.as_ref(), &mut self.sender).await;
                                                result = Some(r);
                                            }
                                        },
                                        Err(e) => {
                                            result = Some(Err(e));
                                        }
                                    };
                                },
                                Err(e) => {
                                    result = Some(Err(e));
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    result = Some(Err(Error::from(e)));
                }
            }
        }
        result
    }

    pub async fn handle_connection(stream: TcpStream) -> Result<WsMessageDispatcher> {
        let ws_stream = accept_async(stream).await?;
        let (sender, receiver) = ws_stream.split();
        let dispatcher = Self::new(Arc::new(Mutex::new(WsSender {
            split_sink: sender,
        })), receiver);
        Ok(dispatcher)
    }
}
