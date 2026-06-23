use std::sync::{Arc, RwLock, atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering}};

use async_trait::async_trait;
use dashmap::DashMap;
use flume::{Receiver, Sender, bounded};
use serde::{Deserialize, Serialize};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::{net::TcpStream, time::{Duration, Instant}};
use tokio_tungstenite::{accept_async, accept_hdr_async, tungstenite::{Message, Utf8Bytes}};
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
    let end = OFFSET_SN + LEN_SN;
    if data.len() < end {
        return Err(Error::BufferTooShort { need: end, got: data.len() });
    }
    let sn_bin: [u8; 4] = data[OFFSET_SN..end].try_into().unwrap();
    Ok(u32::from_be_bytes(sn_bin))
}

pub fn parse_bin_payload_type(data: &[u8]) -> Result<u32> {
    let end = OFFSET_PAYLOAD_TYPE + LEN_PAYLOAD_TYPE;
    if data.len() < end {
        return Err(Error::BufferTooShort { need: end, got: data.len() });
    }
    let type_bin: [u8; 4] = data[OFFSET_PAYLOAD_TYPE..end].try_into().unwrap();
    Ok(u32::from_be_bytes(type_bin))
}

pub fn parse_bin_status_code(data: &[u8]) -> Result<u32> {
    let end = OFFSET_STATUS_CODE + LEN_STATUS_CODE;
    if data.len() < end {
        return Err(Error::BufferTooShort { need: end, got: data.len() });
    }
    let code_bin: [u8; 4] = data[OFFSET_STATUS_CODE..end].try_into().unwrap();
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

/// WSS 握手阶段请求头过滤器。
/// 在 WebSocket Upgrade 完成后、WebSocket 流建立前调用。
/// filter 接收 HTTP Request 的引用，可检查请求头。
/// 返回 Ok(()) 表示通过，返回 Err 表示拒绝连接。
/// 注意：此 trait 是同步的，因为 accept_hdr_async 的回调是同步闭包。
pub trait WsHeaderFilter: Send + Sync {
    fn filter(&self, request: &http::Request<()>) -> Result<()>;
}

async fn ws_handle_json_message(json: Utf8Bytes, recv_ctx: Arc<WsContext>) -> Result<()> {
    let message = serde_json::from_str::<WsMessage>(&json)?;
    let processor = if message.payload_type == TYPE_RESPONSE {
        recv_ctx.reponse_json_processor_map.get(&message.sn)
    } else {
        recv_ctx.request_json_processor_map.get(&message.payload_type)
    };
    if let Some(processor) = processor {
        let proc = processor.clone();
        let ctx = recv_ctx.clone();
        tokio::spawn(async move {
            proc.process_json(message, ctx).await;
        });
    }
    Ok(())
}

async fn ws_handle_bin_message(data: Bytes, recv_ctx: Arc<WsContext>) -> Result<()> {
    let sn = parse_bin_sn(data.as_ref())?;
    let payload_type = parse_bin_payload_type(data.as_ref())?;
    let processor = if payload_type == TYPE_RESPONSE {
        recv_ctx.reponse_bin_processor_map.get(&sn)
    } else {
        recv_ctx.request_bin_processor_map.get(&payload_type)
    };
    if let Some(processor) = processor {
        let proc = processor.clone();
        let ctx = recv_ctx.clone();
        tokio::spawn(async move {
            proc.process_bin(data.as_ref(), ctx).await;
        });
    };
    Ok(())
}

async fn ws_handle_close(recv_ctx: Arc<WsContext>) -> Result<()> {
    //获取关闭回调
    let processor = match recv_ctx.close_processor.read() {
        Ok(guard) => guard.clone(),
        Err(_) => None,
    };
    //执行关闭回调
    if let Some(processor) = processor {
        let ctx = recv_ctx.clone();
        tokio::spawn(async move {
            processor.process_close(ctx).await;
        });
    }
    Ok(())
}

/// 建立 WebSocket 连接，在握手阶段使用 filter 对 HTTP 请求头进行校验。
/// filter 按顺序执行，全部通过才建立连接；任一 filter 拒绝则连接关闭。
pub async fn ws_handle_connection_with_filter<I, P>(stream: TcpStream, queue_capacity: usize, processor_context: Arc<P>, initializer: &I, filters: &[&dyn WsHeaderFilter]) -> Result<()>
where
    I: WsProcessorInitializer<P>,
{
    let ws_stream = if !filters.is_empty() {
        accept_hdr_async(stream, |request: &http::Request<()>, response: http::Response<()>| {
            for filter in filters {
                if let Err(e) = filter.filter(request) {
                    let mut err_response = http::Response::new(Some(e.to_string()));
                    *err_response.status_mut() = http::StatusCode::UNAUTHORIZED;
                    return Err(err_response);
                }
            }
            Ok(response)
        }).await?
    } else {
        accept_async(stream).await?
    };
    let context = Arc::new(WsContext::new(queue_capacity));
    initializer.init(context.clone(), processor_context).await?;
    let (mut sender, mut receiver) = ws_stream.split();
    let recv_ctx = context.clone();
    let send_ctx = context.clone();
    let recv_running = Arc::new(AtomicBool::new(true));
    let send_running = recv_running.clone();

    //处理消息接收
    tokio::spawn(async move {
        let span = span!(Level::INFO, "ws receiving process");
        let _enter = span.enter();
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(msg) => {
                    match msg {
                        Message::Text(json) => {
                            if let Err(e) = ws_handle_json_message(json, recv_ctx.clone()).await {
                                error!("Error handling json message: {:?}", e);
                            }
                        }
                        Message::Binary(data) => {
                            if let Err(e) = ws_handle_bin_message(data, recv_ctx.clone()).await {
                                error!("Error handling bin message: {:?}", e);
                            }
                        }
                        Message::Close(_) => {
                            break;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    error!("Error receiving message: {:?}", e);
                }
            };
        }
        //处理关闭回调
        if let Ok(true) = recv_running.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed) {
            if let Err(e) = ws_handle_close(recv_ctx.clone()).await {
                error!("Error handling close message: {:?}", e);
            }
        }
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
                    match serde_json::to_string(&msg) {
                        Ok(json) => {
                            if let Err(e) = sender.send(Message::text(json)).await {
                                error!("Error sending json message: {:?}", e);
                            }
                        },
                        Err(e) => {
                            error!("Error building json message: {:?}", e);
                        }
                    };
                }
                WsMessageUnion::Binary(msg) => {
                    if let Err(e) = sender.send(Message::binary(msg)).await {
                        error!("Error sending binary message: {:?}", e);
                    }
                }
                WsMessageUnion::Close => {
                    if let Err(e) = sender.send(Message::Close(None)).await {
                        error!("Error sending close message: {:?}", e);
                    }
                }
            }
        }
    });
    Ok(())
}

/// 建立 WebSocket 连接，不使用请求头过滤。
pub async fn ws_handle_connection<I, P>(stream: TcpStream, queue_capacity: usize, processor_context: Arc<P>, initializer: &I) -> Result<()>
where
    I: WsProcessorInitializer<P>,
{
    ws_handle_connection_with_filter(stream, queue_capacity, processor_context, initializer, &[]).await
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicBool, Ordering};
    use bytes::Bytes;
    use super::*;

    // Mock processor for JSON messages - records invocations
    struct MockJsonProcessor {
        called: Arc<Mutex<Vec<WsMessage>>>,
    }

    #[async_trait]
    impl WsJsonProcessor for MockJsonProcessor {
        async fn process_json(&self, data: WsMessage, _context: Arc<WsContext>) {
            self.called.lock().unwrap().push(data);
        }
    }

    // Mock processor for binary messages - records invocations
    struct MockBinProcessor {
        called: Arc<Mutex<Vec<Bytes>>>,
    }

    #[async_trait]
    impl WsBinaryProcessor for MockBinProcessor {
        async fn process_bin(&self, data: &[u8], _context: Arc<WsContext>) {
            self.called.lock().unwrap().push(Bytes::copy_from_slice(data));
        }
    }

    // Mock close processor
    struct MockCloseProcessor {
        called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl WsCloseProcessor for MockCloseProcessor {
        async fn process_close(&self, _context: Arc<WsContext>) {
            self.called.store(true, Ordering::SeqCst);
        }
    }

    // Helper: build a 12-byte binary header
    fn make_bin_header(sn: u32, payload_type: u32, status_code: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12);
        buf.extend_from_slice(&sn.to_be_bytes());
        buf.extend_from_slice(&payload_type.to_be_bytes());
        buf.extend_from_slice(&status_code.to_be_bytes());
        buf
    }

    // Helper: construct a WsMessage
    fn make_message(sn: u32, payload_type: u32, status_code: u32, payload: Option<serde_json::Value>) -> WsMessage {
        WsMessage { sn, payload_type, status_code, payload }
    }

    // === Group 1: Binary parse functions ===

    #[test]
    fn test_parse_bin_sn() {
        let buf = make_bin_header(0x01020304, 0, 0);
        assert_eq!(parse_bin_sn(&buf).unwrap(), 0x01020304);
    }

    #[test]
    fn test_parse_bin_payload_type() {
        let buf = make_bin_header(0, 0xA0B0C0D0, 0);
        assert_eq!(parse_bin_payload_type(&buf).unwrap(), 0xA0B0C0D0);
    }

    #[test]
    fn test_parse_bin_status_code() {
        let buf = make_bin_header(0, 0, 200);
        assert_eq!(parse_bin_status_code(&buf).unwrap(), 200);
    }

    #[test]
    fn test_parse_bin_out_of_bounds() {
        let short = vec![0u8; 3];
        assert!(parse_bin_sn(&short).is_err());
        assert!(parse_bin_payload_type(&short).is_err());
        assert!(parse_bin_status_code(&short).is_err());
    }
}
