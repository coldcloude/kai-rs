use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use async_trait::async_trait;
use dashmap::DashMap;
use flume::{Receiver, Sender, bounded};
use serde::{Deserialize, Serialize};
use bytes::Bytes;
use tokio::{net::TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tracing::{Level, error, span};

use crate::{Error, error::Result};

pub const TYPE_RESPONSE: u32 = 0x00000000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage {
    pub sn: u32,
    pub payload_type: u32,
    pub payload: serde_json::Value,
}

#[async_trait]
pub trait WsBinaryProcessor: Send + Sync + 'static {
    async fn process_bin(&self, data: &[u8], context: Arc<WsContext>) -> Result<()>;
}

#[async_trait]
pub trait WsJsonProcessor: Send + Sync + 'static {
    async fn process_json(&self, data: serde_json::Value, context: Arc<WsContext>) -> Result<()>;
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

pub enum WsMessageUnion {
    Json(WsMessage),
    Binary(Bytes),
    Close,
}

pub struct WsContext {
    sending_queue: (Sender<WsMessageUnion>, Receiver<WsMessageUnion>),
    reponse_bin_processor_map: DashMap<u32, Arc<dyn WsBinaryProcessor>>,
    reponse_json_processor_map: DashMap<u32, Arc<dyn WsJsonProcessor>>,
}

impl WsContext {
    pub fn new(capacity: usize) -> Self {
        Self {
            sending_queue: bounded::<WsMessageUnion>(capacity),
            reponse_bin_processor_map: DashMap::new(),
            reponse_json_processor_map: DashMap::new(),
        }
    }

    pub fn send_json(&self, msg: WsMessage) -> Result<()> {
        self.sending_queue.0.send(WsMessageUnion::Json(msg))?;
        Ok(())
    }

    pub fn send_bin(&self, msg: Bytes) -> Result<()> {
        self.sending_queue.0.send(WsMessageUnion::Binary(msg))?;
        Ok(())
    }

    pub fn send_json_with_json_response(&self, request: WsMessage, response_handler: Arc<dyn WsJsonProcessor>) -> Result<()> {
        self.reponse_json_processor_map.insert(request.sn, response_handler);
        self.send_json(request)
    }

    pub fn send_bin_with_json_response(&self, sn: u32, request: Bytes, response_handler: Arc<dyn WsJsonProcessor>) -> Result<()> {
        self.reponse_json_processor_map.insert(sn, response_handler);
        self.send_bin(request)
    }

    pub fn send_json_with_bin_response(&self, request: WsMessage, response_handler: Arc<dyn WsBinaryProcessor>) -> Result<()> {
        self.reponse_bin_processor_map.insert(request.sn, response_handler);
        self.send_json(request)
    }

    pub fn send_bin_with_bin_response(&self, sn: u32, request: Bytes, response_handler: Arc<dyn WsBinaryProcessor>) -> Result<()> {
        self.reponse_bin_processor_map.insert(sn, response_handler);
        self.send_bin(request)
    }

    pub async fn send_close(&mut self) -> Result<()> {
        self.sending_queue.0.send(WsMessageUnion::Close)?;
        Ok(())
    }
}

pub struct WsMessageDispatcher {
    request_bin_processor_map: DashMap<u32, Arc<dyn WsBinaryProcessor>>,
    request_json_processor_map: DashMap<u32, Arc<dyn WsJsonProcessor>>,
}

impl WsMessageDispatcher {
    pub fn new() -> Self {
        Self {
            request_bin_processor_map: DashMap::new(),
            request_json_processor_map: DashMap::new(),
        }
    }

    pub fn set_bin_processor(&self, payload_type: u32, processor: Arc<dyn WsBinaryProcessor>) {
        self.request_bin_processor_map.insert(payload_type, processor);
    }

    pub fn set_json_processor(&self, payload_type: u32, processor: Arc<dyn WsJsonProcessor>) {
        self.request_json_processor_map.insert(payload_type, processor);
    }

    pub async fn handle_connection(dispatcher: Arc<WsMessageDispatcher>, stream: TcpStream, queue_capacity: usize) -> Result<Arc<WsContext>> {
        let ws_stream = accept_async(stream).await?;
        let (mut sender, mut receiver) = ws_stream.split();
        let context = Arc::new(WsContext::new(queue_capacity));
        let recv_ctx = context.clone();
        let send_ctx = context.clone();
        let recv_running = Arc::new(AtomicBool::new(true));
        let send_running = recv_running.clone();
        tokio::spawn(async move {
            let span = span!(Level::INFO, "ws receiving process");
            let _enter = span.enter();
            while let Some(msg) = receiver.next().await {
                let mut result = Ok(());
                match msg {
                    Ok(msg) => {
                        match msg {
                            Message::Text(json) => {
                                match serde_json::from_str::<WsMessage>(&json) {
                                    Ok(message) => {
                                        let processor = if message.payload_type == TYPE_RESPONSE {
                                            recv_ctx.reponse_json_processor_map.get(&message.sn)
                                        } else {
                                            dispatcher.request_json_processor_map.get(&message.payload_type)
                                        };
                                        if let Some(processor) = processor {
                                            let proc = processor.clone();
                                            let ctx = recv_ctx.clone();
                                            tokio::spawn(async move {
                                                let span = span!(Level::INFO, "ws processing JSON message");
                                                let _enter = span.enter();
                                                if let Err(e) = proc.process_json(message.payload, ctx).await {
                                                    error!("Error processing JSON message: {:?}", e);
                                                }
                                            });
                                        }
                                    },
                                    Err(e) => {
                                        result = Err(Error::from(e));
                                    }
                                };
                            }
                            Message::Binary(data) => {
                                match parse_bin_sn(data.as_ref()) {
                                    Ok(sn) => {
                                        match parse_bin_payload_type(data.as_ref()) {
                                            Ok(payload_type) => {
                                                let processor = if payload_type == TYPE_RESPONSE {
                                                    recv_ctx.reponse_bin_processor_map.get(&sn)
                                                } else {
                                                    dispatcher.request_bin_processor_map.get(&payload_type)
                                                };
                                                if let Some(processor) = processor {
                                                    let proc = processor.clone();
                                                    let ctx = recv_ctx.clone();
                                                    tokio::spawn(async move {
                                                        let span = span!(Level::INFO, "ws processing binary message");
                                                        let _enter = span.enter();
                                                        if let Err(e) = proc.process_bin(data.as_ref(), ctx).await {
                                                            error!("Error processing binary message: {:?}", e);
                                                        }
                                                    });
                                                }
                                            },
                                            Err(e) => {
                                                result = Err(e);
                                            }
                                        };
                                    },
                                    Err(e) => {
                                        result = Err(e);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        result = Err(Error::from(e));
                    }
                }
                if let Err(e) = result {
                    error!("Error receiving message: {:?}", e);
                    break;
                }
            }
            recv_running.store(false, Ordering::Relaxed);
        });
        tokio::task::spawn(async move {
            let span = span!(Level::INFO, "ws sending process");
            let _enter = span.enter();
            while send_running.load(Ordering::Relaxed) {
                let result = send_ctx.sending_queue.1.recv_async().await;
                if let Ok(msg) = result {
                    let mut result = Ok(());
                    match msg {
                        WsMessageUnion::Json(msg) => {
                            match serde_json::to_string(&msg) {
                                Ok(json) => {
                                    if let Err(e) = sender.send(Message::text(json)).await {
                                        result = Err(Error::from(e));
                                    }
                                },
                                Err(e) => {
                                    result = Err(Error::from(e));
                                }
                            }
                        }
                        WsMessageUnion::Binary(msg) => {
                            if let Err(e) = sender.send(Message::binary(msg)).await {
                                result = Err(Error::from(e));
                            }
                        }
                        WsMessageUnion::Close => {
                            break;
                        }
                    }
                    if let Err(e) = result {
                        error!("Error sending message: {:?}", e);
                        break;
                    }
                }
                else {
                    break;
                }
            }
        });
        Ok(context)
    }
}
