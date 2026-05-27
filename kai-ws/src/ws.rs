use std::sync::{Arc, RwLock, atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering}};

use async_trait::async_trait;
use dashmap::DashMap;
use flume::{Receiver, Sender, bounded};
use serde::{Deserialize, Serialize};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::{net::TcpStream, time::{Duration, Instant}};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tracing::{Level, error, span};

use crate::{Error, error::Result};

pub const TYPE_RESPONSE: u32 = 0x00000000;

pub const TYPE_HEARTBEAT: u32 = 0x00000000;

pub const CODE_SUCCESS: u32 = 200;
pub const CODE_ERROR: u32 = 500;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage {
    pub sn: u32,
    pub payload_type: u32,
    pub status_code: u32,
    pub payload: Option<serde_json::Value>,
}

#[async_trait]
pub trait WsBinaryProcessor: Send + Sync + 'static {
    async fn process_bin(&self, data: &[u8], context: Arc<WsContext>);
}

#[async_trait]
pub trait WsJsonProcessor: Send + Sync + 'static {
    async fn process_json(&self, data: WsMessage, context: Arc<WsContext>);
}

#[async_trait]
pub trait WsCloseProcessor: Send + Sync + 'static {
    async fn process_close(&self, context: Arc<WsContext>);
}

pub const OFFSET_SN: usize = 0;
pub const LEN_SN: usize = 4;

pub const OFFSET_PAYLOAD_TYPE: usize = OFFSET_SN + LEN_SN;
pub const LEN_PAYLOAD_TYPE: usize = 4;

pub const OFFSET_STATUS_CODE: usize = OFFSET_PAYLOAD_TYPE + LEN_PAYLOAD_TYPE;
pub const LEN_STATUS_CODE: usize = 4;

pub fn parse_bin_sn(data: &[u8]) -> Result<u32> {
    let sn_bin: [u8; 4] = data[OFFSET_SN..OFFSET_SN + LEN_SN].try_into()?;
    Ok(u32::from_be_bytes(sn_bin))
}

pub fn parse_bin_payload_type(data: &[u8]) -> Result<u32> {
    let type_bin: [u8; 4] = data[OFFSET_PAYLOAD_TYPE..OFFSET_PAYLOAD_TYPE + LEN_PAYLOAD_TYPE].try_into()?;
    Ok(u32::from_be_bytes(type_bin))
}

pub fn parse_bin_status_code(data: &[u8]) -> Result<u32> {
    let code_bin: [u8; 4] = data[OFFSET_STATUS_CODE..OFFSET_STATUS_CODE + LEN_STATUS_CODE].try_into()?;
    Ok(u32::from_be_bytes(code_bin))
}

pub enum WsMessageUnion {
    Json(WsMessage),
    Binary(Bytes),
    Close,
}

pub struct WsContext {
    request_sn: AtomicU32,
    sending_queue: (Sender<WsMessageUnion>, Receiver<WsMessageUnion>),
    request_bin_processor_map: DashMap<u32, Arc<dyn WsBinaryProcessor>>,
    request_json_processor_map: DashMap<u32, Arc<dyn WsJsonProcessor>>,
    close_processor: Arc<RwLock<Option<Arc<dyn WsCloseProcessor>>>>,
    reponse_bin_processor_map: DashMap<u32, Arc<dyn WsBinaryProcessor>>,
    reponse_json_processor_map: DashMap<u32, Arc<dyn WsJsonProcessor>>,
}

impl WsContext {
    pub fn new(capacity: usize) -> Self {
        Self {
            request_sn: AtomicU32::new(0),
            sending_queue: bounded::<WsMessageUnion>(capacity),
            request_bin_processor_map: DashMap::new(),
            request_json_processor_map: DashMap::new(),
            close_processor: Arc::new(RwLock::new(None)),
            reponse_bin_processor_map: DashMap::new(),
            reponse_json_processor_map: DashMap::new(),
        }
    }

    pub fn next_request_sn(&self) -> u32 {
        self.request_sn.fetch_add(1, Ordering::Relaxed)
    }

    pub fn set_bin_processor(&self, payload_type: u32, processor: Arc<dyn WsBinaryProcessor>) {
        self.request_bin_processor_map.insert(payload_type, processor);
    }

    pub fn set_json_processor(&self, payload_type: u32, processor: Arc<dyn WsJsonProcessor>) {
        self.request_json_processor_map.insert(payload_type, processor);
    }

    pub fn set_close_processor(&self, processor: Arc<dyn WsCloseProcessor>) {
        if let Ok(mut guard) = self.close_processor.write() {
            *guard = Some(processor);
        }
    }

    pub async fn send_json(&self, msg: WsMessage) -> Result<()> {
        self.sending_queue.0.send_async(WsMessageUnion::Json(msg)).await?;
        Ok(())
    }

    pub async fn send_bin(&self, msg: Bytes) -> Result<()> {
        self.sending_queue.0.send_async(WsMessageUnion::Binary(msg)).await?;
        Ok(())
    }

    pub async fn send_json_with_json_response(&self, request: WsMessage, response_handler: Arc<dyn WsJsonProcessor>) -> Result<()> {
        self.reponse_json_processor_map.insert(request.sn, response_handler);
        self.send_json(request).await
    }

    pub async fn send_bin_with_json_response(&self, sn: u32, request: Bytes, response_handler: Arc<dyn WsJsonProcessor>) -> Result<()> {
        self.reponse_json_processor_map.insert(sn, response_handler);
        self.send_bin(request).await
    }

    pub async fn send_json_with_bin_response(&self, request: WsMessage, response_handler: Arc<dyn WsBinaryProcessor>) -> Result<()> {
        self.reponse_bin_processor_map.insert(request.sn, response_handler);
        self.send_json(request).await
    }

    pub async fn send_bin_with_bin_response(&self, sn: u32, request: Bytes, response_handler: Arc<dyn WsBinaryProcessor>) -> Result<()> {
        self.reponse_bin_processor_map.insert(sn, response_handler);
        self.send_bin(request).await
    }

    pub async fn send_close(&self) -> Result<()> {
        self.sending_queue.0.send_async(WsMessageUnion::Close).await?;
        Ok(())
    }
}

#[async_trait]
pub trait WsProcessorInitializer<P>: Send + Sync {
    async fn init(&self, ws_context: Arc<WsContext>, processor_context: Arc<P>) -> Result<()>;
}

pub async fn ws_handle_connection<I, P>(stream: TcpStream, queue_capacity: usize, processor_context: Arc<P>, initializer: &I) -> Result<()>
where
    I: WsProcessorInitializer<P>,
{
    let ws_stream = accept_async(stream).await?;
    let (mut sender, mut receiver) = ws_stream.split();
    let context = Arc::new(WsContext::new(queue_capacity));
    initializer.init(context.clone(), processor_context).await?;

    let recv_ctx = context.clone();
    let send_ctx = context.clone();
    let close_ctx = context.clone();
    let recv_running = Arc::new(AtomicBool::new(true));
    let send_running = recv_running.clone();

    //处理消息接收
    tokio::spawn(async move {
        let span = span!(Level::INFO, "ws receiving process");
        let _enter = span.enter();
        while let Some(msg) = receiver.next().await {
            let msg = match msg {
                Ok(msg) => {
                    msg
                }
                Err(e) => {
                    error!("Error receiving message: {:?}", e);
                    continue;
                }
            };
            match msg {
                Message::Text(json) => {
                    let message = match serde_json::from_str::<WsMessage>(&json) {
                        Ok(message) => {
                            message
                        },
                        Err(e) => {
                            error!("Error parsing json message: {:?}", e);
                            continue;
                        }
                    };
                    let processor = if message.payload_type == TYPE_RESPONSE {
                        recv_ctx.reponse_json_processor_map.get(&message.sn)
                    } else {
                        recv_ctx.request_json_processor_map.get(&message.payload_type)
                    };
                    let Some(processor) = processor else {
                        continue;
                    };
                    let proc = processor.clone();
                    let ctx = recv_ctx.clone();
                    tokio::spawn(async move {
                        proc.process_json(message, ctx).await;
                    });
                }
                Message::Binary(data) => {
                    let sn = match parse_bin_sn(data.as_ref()) {
                        Ok(sn) => {
                            sn
                        },
                        Err(e) => {
                            error!("Error parsing binary payload sn: {:?}", e);
                            continue;
                        }
                    };
                    let payload_type = match parse_bin_payload_type(data.as_ref()) {
                        Ok(payload_type) => {
                            payload_type
                        },
                        Err(e) => {
                            error!("Error parsing binary payload type: {:?}", e);
                            continue;
                        }
                    };
                    let processor = if payload_type == TYPE_RESPONSE {
                        recv_ctx.reponse_bin_processor_map.get(&sn)
                    } else {
                        recv_ctx.request_bin_processor_map.get(&payload_type)
                    };
                    let Some(processor) = processor else {
                        continue;
                    };
                    let proc = processor.clone();
                    let ctx = recv_ctx.clone();
                    tokio::spawn(async move {
                        proc.process_bin(data.as_ref(), ctx).await;
                    });
                }
                Message::Close(_) => {
                    break;
                }
                _ => {}
            }
        }

        //处理关闭回调
        let Err(_) = recv_running.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed) else {
            return;
        };
        //获取关闭回调
        let Ok(guard) = recv_ctx.close_processor.read() else {
            return;
        };
        let Some(processor) = guard.clone() else {
            return;
        };
        //执行关闭回调
        tokio::spawn(async move {
            processor.process_close(close_ctx).await;
        });
    });

    //处理消息发送
    tokio::task::spawn(async move {
        let span = span!(Level::INFO, "ws sending process");
        let _enter = span.enter();
        while send_running.load(Ordering::Relaxed) {
            let Ok(msg) = send_ctx.sending_queue.1.recv_async().await else {
                break;
            };
            match msg {
                WsMessageUnion::Json(msg) => {
                    let json = match serde_json::to_string(&msg) {
                        Ok(json) => {
                            json
                        },
                        Err(e) => {
                            error!("Error building json message: {:?}", e);
                            continue;
                        }
                    };
                    if let Err(e) = sender.send(Message::text(json)).await {
                            error!("Error sending json message: {:?}", e);
                            continue;
                    }
                }
                WsMessageUnion::Binary(msg) => {
                    if let Err(e) = sender.send(Message::binary(msg)).await {
                        error!("Error sending binary message: {:?}", e);
                        continue;
                    }
                }
                WsMessageUnion::Close => {
                    if let Err(e) = sender.send(Message::Close(None)).await {
                        error!("Error sending close message: {:?}", e);
                        continue;
                    }
                }
            }
        }
    });
    Ok(())
}

pub struct WsHeartbeatHandler {
    anchor: Instant,
    interval: Duration,
    deadline: AtomicU64,
    next_send: AtomicU64,
    running: AtomicBool,
    ws_context: Arc<WsContext>,
}

impl WsHeartbeatHandler {
    fn instant_as_u64(&self, instant: Instant) -> u64 {
        instant.duration_since(self.anchor).as_millis() as u64
    }

    fn update_deadline(&self) {
        let deadline = self.instant_as_u64(Instant::now() + self.interval * 3);
        self.deadline.store(deadline, Ordering::Relaxed);
    }

    fn update_next_send(&self) {
        let next_send = self.instant_as_u64(Instant::now() + self.interval);
        self.next_send.store(next_send, Ordering::Relaxed);
    }

    fn u64_as_instant(&self, duration: u64) -> Instant {
        self.anchor + Duration::from_millis(duration)
    }

    fn get_deadline(&self) -> Instant {
        self.u64_as_instant(self.deadline.load(Ordering::Relaxed))
    }

    fn get_next_send(&self) -> Instant {
        self.u64_as_instant(self.next_send.load(Ordering::Relaxed))
    }

    pub fn new(interval: Duration, ws_context: Arc<WsContext>) -> Self {
        Self {
            anchor: Instant::now(),
            interval,
            deadline: AtomicU64::new(0),
            next_send: AtomicU64::new(0),
            running: AtomicBool::new(false),
            ws_context,
        }
    }

    pub async fn start(&self) -> Result<()> {
        if let Err(_) = self.running.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed) {
            return Err(Error::HeartbeatHandlerAlreadyStarted);
        }
        self.update_deadline();
        self.update_next_send();
        while self.running.load(Ordering::Relaxed) {
            let now = Instant::now();
            //先处理超时关闭
            let deadline = self.get_deadline();
            if deadline < now {
                let span = span!(Level::INFO, "ws heartbeat timeout");
                let _enter = span.enter();
                self.running.store(false, Ordering::Relaxed);
                if let Err(e) = self.ws_context.send_close().await {
                    error!("Error sending close: {:?}", e);
                }
                break;
            }
            //再处理心跳发送
            let next_send = self.get_next_send();
            if next_send < now {
                let span = span!(Level::INFO, "ws heartbeat send");
                let _enter = span.enter();
                let mut buffer = BytesMut::new();
                let sn = self.ws_context.next_request_sn();
                buffer.put_u32(sn);
                buffer.put_u32(TYPE_HEARTBEAT);
                if let Err(e) = self.ws_context.send_bin(buffer.freeze()).await {
                    error!("Error sending heartbeat: {:?}", e);
                }
                self.update_next_send();
            }
            //空闲至下次事件
            let next_send = self.get_next_send();
            let min_next_send = next_send.min(deadline);
            tokio::time::sleep(min_next_send - now).await;
        }
        Ok(())
    }
}

#[async_trait]
impl WsBinaryProcessor for WsHeartbeatHandler {
    //收到数据后，更新timeout
    async fn process_bin(&self, _: &[u8], _: Arc<WsContext>) {
        if self.running.load(Ordering::Relaxed) {
            self.update_deadline();
        }
    }
}
