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
    async fn process_bin(&self, data: Bytes, context: Arc<WsContext>);
}

#[async_trait]
pub trait WsJsonProcessor: Send + Sync + 'static {
    async fn process_json(&self, data: WsMessage, context: Arc<WsContext>);
}

#[async_trait]
pub trait WsBinaryProcessorMut: Send + Sync + 'static {
    async fn process_bin(&mut self, data: Bytes, context: Arc<WsContext>);
}

#[async_trait]
pub trait WsJsonProcessorMut: Send + Sync + 'static {
    async fn process_json(&mut self, data: WsMessage, context: Arc<WsContext>);
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
    let sn_bytes: [u8; 4] = data.get(OFFSET_SN..OFFSET_SN + LEN_SN)
        .and_then(|s| s.try_into().ok())
        .ok_or(Error::BinParse)?;
    Ok(u32::from_be_bytes(sn_bytes))
}

pub fn parse_bin_payload_type(data: &[u8]) -> Result<u32> {
    let type_bytes: [u8; 4] = data.get(OFFSET_PAYLOAD_TYPE..OFFSET_PAYLOAD_TYPE + LEN_PAYLOAD_TYPE)
        .and_then(|s| s.try_into().ok())
        .ok_or(Error::BinParse)?;
    Ok(u32::from_be_bytes(type_bytes))
}

pub fn parse_bin_status_code(data: &[u8]) -> Result<u32> {
    let code_bytes: [u8; 4] = data.get(OFFSET_STATUS_CODE..OFFSET_STATUS_CODE + LEN_STATUS_CODE)
        .and_then(|s| s.try_into().ok())
        .ok_or(Error::BinParse)?;
    Ok(u32::from_be_bytes(code_bytes))
}

#[derive(Debug)]
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
    reponse_bin_processor_map: DashMap<u32, Box<dyn WsBinaryProcessorMut>>,
    reponse_json_processor_map: DashMap<u32, Box<dyn WsJsonProcessorMut>>,
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

    pub async fn send_json_with_json_response(&self, request: WsMessage, response_handler: Box<dyn WsJsonProcessorMut>) -> Result<()> {
        self.reponse_json_processor_map.insert(request.sn, response_handler);
        self.send_json(request).await
    }

    pub async fn send_bin_with_json_response(&self, sn: u32, request: Bytes, response_handler: Box<dyn WsJsonProcessorMut>) -> Result<()> {
        self.reponse_json_processor_map.insert(sn, response_handler);
        self.send_bin(request).await
    }

    pub async fn send_json_with_bin_response(&self, request: WsMessage, response_handler: Box<dyn WsBinaryProcessorMut>) -> Result<()> {
        self.reponse_bin_processor_map.insert(request.sn, response_handler);
        self.send_json(request).await
    }

    pub async fn send_bin_with_bin_response(&self, sn: u32, request: Bytes, response_handler: Box<dyn WsBinaryProcessorMut>) -> Result<()> {
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
/// 返回 Ok(()) 表示通过，返回 Err 表示拒绝连接（携带完整的 HTTP 错误响应）。
/// 注意：此 trait 是同步的，因为 accept_hdr_async 的回调是同步闭包。
pub trait WsHeaderFilter: Send + Sync {
    fn filter(&self, request: &http::Request<()>) -> std::result::Result<(), http::Response<Option<String>>>;
}

async fn ws_handle_json_message(json: Utf8Bytes, recv_ctx: Arc<WsContext>) -> Result<()> {
    let message = serde_json::from_str::<WsMessage>(&json)?;
    if message.payload_type == TYPE_RESPONSE {
        if let Some((_,mut processor)) = recv_ctx.reponse_json_processor_map.remove(&message.sn) {
        let ctx = recv_ctx.clone();
            tokio::spawn(async move {
                processor.process_json(message, ctx).await;
            });
        }
    } else {
        if let Some(processor) = recv_ctx.request_json_processor_map.get(&message.payload_type) {
            let proc = processor.clone();
            let ctx = recv_ctx.clone();
            tokio::spawn(async move {
                proc.process_json(message, ctx).await;
            });
        }
    };
    Ok(())
}

async fn ws_handle_bin_message(data: Bytes, recv_ctx: Arc<WsContext>) -> Result<()> {
    let sn = parse_bin_sn(data.as_ref())?;
    let payload_type = parse_bin_payload_type(data.as_ref())?;
    if payload_type == TYPE_RESPONSE {
        if let Some((_,mut processor)) = recv_ctx.reponse_bin_processor_map.remove(&sn) {
            let ctx = recv_ctx.clone();
            tokio::spawn(async move {
                processor.process_bin(data, ctx).await;
            });
        }
    } else {
        if let Some(processor) = recv_ctx.request_bin_processor_map.get(&payload_type) {
            let proc = processor.clone();
            let ctx = recv_ctx.clone();
            tokio::spawn(async move {
                proc.process_bin(data, ctx).await;
            });
        }
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
                if let Err(err_response) = filter.filter(request) {
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
    async fn process_bin(&self, _: Bytes, _: Arc<WsContext>) {
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
    use tokio::time::Duration;
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
        async fn process_bin(&self, data: Bytes, _context: Arc<WsContext>) {
            self.called.lock().unwrap().push(data);
        }
    }

    // Mock Mut processors for response handler tests
    struct MockJsonProcessorMut {
        called: Arc<Mutex<Vec<WsMessage>>>,
    }

    #[async_trait]
    impl WsJsonProcessorMut for MockJsonProcessorMut {
        async fn process_json(&mut self, data: WsMessage, _context: Arc<WsContext>) {
            self.called.lock().unwrap().push(data);
        }
    }

    struct MockBinProcessorMut {
        called: Arc<Mutex<Vec<Bytes>>>,
    }

    #[async_trait]
    impl WsBinaryProcessorMut for MockBinProcessorMut {
        async fn process_bin(&mut self, data: Bytes, _context: Arc<WsContext>) {
            self.called.lock().unwrap().push(data);
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

    // === Group 2: WsMessage serde ===

    #[test]
    fn test_ws_message_roundtrip() {
        let msg = make_message(42, 100, 200, Some(serde_json::json!({"key": "value"})));
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: WsMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.sn, 42);
        assert_eq!(decoded.payload_type, 100);
        assert_eq!(decoded.status_code, 200);
        assert_eq!(decoded.payload, Some(serde_json::json!({"key": "value"})));
    }

    #[test]
    fn test_ws_message_payload_none() {
        let msg = make_message(1, 2, 3, None);
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: WsMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.sn, 1);
        assert_eq!(decoded.payload_type, 2);
        assert_eq!(decoded.status_code, 3);
        assert!(decoded.payload.is_none());
    }

    #[test]
    fn test_ws_message_payload_value() {
        let msg = make_message(0, 0, 0, Some(serde_json::json!({
            "nested": {"array": [1, 2, 3]},
            "flag": true
        })));
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: WsMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.payload.as_ref().unwrap()["nested"]["array"], serde_json::json!([1, 2, 3]));
        assert_eq!(decoded.payload.as_ref().unwrap()["flag"], serde_json::json!(true));
    }

    // === Group 3: WsContext core methods ===

    #[tokio::test]
    async fn test_context_new() {
        let ctx = WsContext::new(16);
        // Queue should be empty initially
        let result = tokio::time::timeout(Duration::from_millis(100), ctx.sending_queue.1.recv_async()).await;
        assert!(result.is_err(), "queue should be empty after construction");
    }

    #[tokio::test]
    async fn test_context_next_sn() {
        let ctx = WsContext::new(16);
        assert_eq!(ctx.next_request_sn(), 0);
        assert_eq!(ctx.next_request_sn(), 1);
        assert_eq!(ctx.next_request_sn(), 2);
        assert_eq!(ctx.next_request_sn(), 3);
    }

    #[tokio::test]
    async fn test_context_set_bin_processor() {
        let ctx = Arc::new(WsContext::new(16));
        let proc = Arc::new(MockBinProcessor { called: Arc::new(Mutex::new(Vec::new())) });
        ctx.set_bin_processor(100, proc.clone());
        // Verify it's in the map by dispatching a binary message via ws_handle_bin_message
        let data = Bytes::from(make_bin_header(1, 100, 200));
        ws_handle_bin_message(data, ctx.clone()).await.unwrap();
        // Give the spawned task time to execute
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(proc.called.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_context_set_json_processor() {
        let ctx = Arc::new(WsContext::new(16));
        let calls: Arc<Mutex<Vec<WsMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let proc = Arc::new(MockJsonProcessor { called: calls.clone() });
        ctx.set_json_processor(200, proc);
        let json = serde_json::to_string(&make_message(1, 200, 200, None)).unwrap();
        let utf8_bytes: Utf8Bytes = json.into();
        ws_handle_json_message(utf8_bytes, ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(calls.lock().unwrap()[0].payload_type, 200);
    }

    #[tokio::test]
    async fn test_context_set_close_processor() {
        let ctx = Arc::new(WsContext::new(16));
        let flag = Arc::new(AtomicBool::new(false));
        let proc = Arc::new(MockCloseProcessor { called: flag.clone() });
        ctx.set_close_processor(proc);
        ws_handle_close(ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_context_send_json() {
        let ctx = WsContext::new(16);
        let msg = make_message(1, 2, 3, None);
        ctx.send_json(msg.clone()).await.unwrap();
        let received = ctx.sending_queue.1.recv_async().await.unwrap();
        match received {
            WsMessageUnion::Json(m) => {
                assert_eq!(m.sn, 1);
                assert_eq!(m.payload_type, 2);
            },
            _ => panic!("expected Json variant"),
        }
    }

    #[tokio::test]
    async fn test_context_send_bin() {
        let ctx = WsContext::new(16);
        let data = Bytes::from(vec![1, 2, 3]);
        ctx.send_bin(data.clone()).await.unwrap();
        let received = ctx.sending_queue.1.recv_async().await.unwrap();
        match received {
            WsMessageUnion::Binary(b) => {
                assert_eq!(b.as_ref(), &[1, 2, 3]);
            },
            _ => panic!("expected Binary variant"),
        }
    }

    #[tokio::test]
    async fn test_context_send_close() {
        let ctx = WsContext::new(16);
        ctx.send_close().await.unwrap();
        let received = ctx.sending_queue.1.recv_async().await.unwrap();
        assert!(matches!(received, WsMessageUnion::Close));
    }

    #[tokio::test]
    async fn test_context_response_handlers() {
        let ctx = Arc::new(WsContext::new(16));

        // Test send_json_with_json_response
        let json_calls: Arc<Mutex<Vec<WsMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let json_proc = MockJsonProcessorMut { called: json_calls.clone() };
        let req = make_message(10, 1, 200, None);
        ctx.send_json_with_json_response(req.clone(), Box::new(json_proc)).await.unwrap();

        // Verify the request was sent, then dispatch a response (TYPE_RESPONSE)
        let sent = ctx.sending_queue.1.recv_async().await.unwrap();
        assert!(matches!(sent, WsMessageUnion::Json(_)));

        let resp = serde_json::to_string(&make_message(10, TYPE_RESPONSE, 200, None)).unwrap();
        let utf8_bytes: Utf8Bytes = resp.into();
        ws_handle_json_message(utf8_bytes, ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(json_calls.lock().unwrap().len(), 1);

        // Test send_bin_with_bin_response
        let bin_calls: Arc<Mutex<Vec<Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        let bin_proc = MockBinProcessorMut { called: bin_calls.clone() };
        let bin_req = Bytes::from(make_bin_header(20, 2, 200));
        ctx.send_bin_with_bin_response(20, bin_req, Box::new(bin_proc)).await.unwrap();

        let sent_bin = ctx.sending_queue.1.recv_async().await.unwrap();
        assert!(matches!(sent_bin, WsMessageUnion::Binary(_)));

        let resp_bin = Bytes::from(make_bin_header(20, TYPE_RESPONSE, 200));
        ws_handle_bin_message(resp_bin, ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(bin_calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_context_send_queue_full() {
        let ctx = WsContext::new(1);  // capacity = 1
        let data = Bytes::from(vec![1]);
        ctx.send_bin(data).await.unwrap();
        // Queue is full now; send should block (never return error or panic)
        // We just verify the first message is receivable
        let received = ctx.sending_queue.1.recv_async().await.unwrap();
        assert!(matches!(received, WsMessageUnion::Binary(_)));
    }

    // === Group 4: Message dispatch logic ===

    #[tokio::test]
    async fn test_handle_json_request() {
        let ctx = Arc::new(WsContext::new(16));
        let calls: Arc<Mutex<Vec<WsMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let proc = Arc::new(MockJsonProcessor { called: calls.clone() });
        ctx.set_json_processor(99, proc);

        let msg = make_message(1, 99, 200, None);
        let json = serde_json::to_string(&msg).unwrap();
        let utf8_bytes: Utf8Bytes = json.into();
        ws_handle_json_message(utf8_bytes, ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(calls.lock().unwrap()[0].sn, 1);
    }

    #[tokio::test]
    async fn test_handle_json_response() {
        let ctx = Arc::new(WsContext::new(16));
        let calls: Arc<Mutex<Vec<WsMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let proc = MockJsonProcessorMut { called: calls.clone() };
        // Register as response processor via send_json_with_json_response
        let req = make_message(5, 1, 200, None);
        ctx.send_json_with_json_response(req, Box::new(proc)).await.unwrap();
        // Drain the sent request from queue
        let _ = ctx.sending_queue.1.recv_async().await.unwrap();

        let msg = make_message(5, TYPE_RESPONSE, 200, None);
        let json = serde_json::to_string(&msg).unwrap();
        let utf8_bytes: Utf8Bytes = json.into();
        ws_handle_json_message(utf8_bytes, ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(calls.lock().unwrap()[0].sn, 5);
    }

    #[tokio::test]
    async fn test_handle_bin_request() {
        let ctx = Arc::new(WsContext::new(16));
        let calls: Arc<Mutex<Vec<Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        let proc = Arc::new(MockBinProcessor { called: calls.clone() });
        ctx.set_bin_processor(77, proc);

        let data = Bytes::from(make_bin_header(1, 77, 200));
        ws_handle_bin_message(data.clone(), ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_handle_bin_response() {
        let ctx = Arc::new(WsContext::new(16));
        let calls: Arc<Mutex<Vec<Bytes>>> = Arc::new(Mutex::new(Vec::new()));
        let proc = MockBinProcessorMut { called: calls.clone() };
        // Register as response processor via send_bin_with_bin_response
        let req = Bytes::from(make_bin_header(8, 1, 200));
        ctx.send_bin_with_bin_response(8, req, Box::new(proc)).await.unwrap();
        // Drain the sent request from queue
        let _ = ctx.sending_queue.1.recv_async().await.unwrap();

        let data = Bytes::from(make_bin_header(8, TYPE_RESPONSE, 200));
        ws_handle_bin_message(data.clone(), ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_handle_close() {
        let ctx = Arc::new(WsContext::new(16));
        let flag = Arc::new(AtomicBool::new(false));
        let proc = Arc::new(MockCloseProcessor { called: flag.clone() });
        ctx.set_close_processor(proc);

        ws_handle_close(ctx.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_handle_unregistered_type() {
        let ctx = Arc::new(WsContext::new(16));

        // JSON with unregistered payload_type - should not panic
        let msg = make_message(1, 999, 200, None);
        let json = serde_json::to_string(&msg).unwrap();
        let utf8_bytes: Utf8Bytes = json.into();
        let result = ws_handle_json_message(utf8_bytes, ctx.clone()).await;
        assert!(result.is_ok());

        // Binary with unregistered payload_type - should not panic
        let data = Bytes::from(make_bin_header(1, 999, 200));
        let result = ws_handle_bin_message(data, ctx.clone()).await;
        assert!(result.is_ok());
    }

    // === Group 5: WsHeartbeatHandler integration tests ===
    // All use 1-second interval, wrapped in tokio::time::timeout
    // to prevent hanging.

    #[tokio::test]
    async fn test_heartbeat_send() {
        let ctx = Arc::new(WsContext::new(16));
        let handler = Arc::new(WsHeartbeatHandler::new(Duration::from_secs(1), ctx.clone()));

        tokio::time::timeout(Duration::from_secs(5), async {
            // Start heartbeat in a background task
            let h = handler.clone();
            tokio::spawn(async move {
                let _ = h.start().await;
            });

            // Receive from queue - should get a heartbeat within ~1.1s
            tokio::time::sleep(Duration::from_millis(1100)).await;
            let received = ctx.sending_queue.1.try_recv();

            // Stop the heartbeat
            handler.running.store(false, Ordering::Relaxed);

            match received {
                Ok(WsMessageUnion::Binary(data)) => {
                    let sn = parse_bin_sn(&data).unwrap();
                    let pt = parse_bin_payload_type(&data).unwrap();
                    assert_eq!(pt, TYPE_HEARTBEAT, "payload_type should be TYPE_HEARTBEAT");
                    assert!(sn <= 1, "sn should be 0 or 1");
                },
                other => panic!("expected Binary heartbeat, got {:?}", other),
            }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_refresh() {
        let ctx = Arc::new(WsContext::new(16));
        let handler = Arc::new(WsHeartbeatHandler::new(Duration::from_secs(1), ctx.clone()));

        tokio::time::timeout(Duration::from_secs(8), async {
            let h = handler.clone();
            tokio::spawn(async move {
                let _ = h.start().await;
            });

            // Feed data periodically to refresh deadline
            for _ in 0..4 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                handler.process_bin(Bytes::from_static(&[0u8; 12]), ctx.clone()).await;
            }

            // After ~2s with refreshes, no close should have been sent
            // Drain any heartbeat messages
            while ctx.sending_queue.1.try_recv().is_ok() {}

            // Give more time - if deadline wasn't refreshed, close would be sent
            tokio::time::sleep(Duration::from_secs(2)).await;
            let result = ctx.sending_queue.1.try_recv();
            // We may get a heartbeat or nothing; we should NOT get a Close
            match result {
                Ok(WsMessageUnion::Close) => panic!("should not have timed out after refresh"),
                _ => {},  // OK: either heartbeat or empty
            }

            handler.running.store(false, Ordering::Relaxed);
        }).await.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_timeout() {
        let ctx = Arc::new(WsContext::new(16));
        let handler = Arc::new(WsHeartbeatHandler::new(Duration::from_secs(1), ctx.clone()));

        tokio::time::timeout(Duration::from_secs(8), async {
            let h = handler.clone();
            tokio::spawn(async move {
                let _ = h.start().await;
            });

            // Wait for timeout (~3s deadline + slop)
            tokio::time::sleep(Duration::from_millis(3500)).await;

            assert!(handler.running.load(Ordering::Relaxed) == false, "handler should have stopped");
        }).await.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_already_started() {
        let ctx = Arc::new(WsContext::new(16));
        let handler = Arc::new(WsHeartbeatHandler::new(Duration::from_secs(1), ctx.clone()));

        let h = handler.clone();
        tokio::spawn(async move {
            let _ = h.start().await;
        });

        // Give time for the first start to set running=true
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second start should fail
        let result = handler.start().await;
        assert!(matches!(result, Err(crate::Error::HeartbeatHandlerAlreadyStarted)));
        handler.running.store(false, Ordering::Relaxed);
    }

    // === Group 6: WsHeaderFilter tests ===

    struct AcceptFilter;

    impl WsHeaderFilter for AcceptFilter {
        fn filter(&self, _request: &http::Request<()>) -> std::result::Result<(), http::Response<Option<String>>> {
            Ok(())
        }
    }

    struct RejectFilter;

    impl WsHeaderFilter for RejectFilter {
        fn filter(&self, _request: &http::Request<()>) -> std::result::Result<(), http::Response<Option<String>>> {
            Err(http::Response::builder()
                .status(http::StatusCode::UNAUTHORIZED)
                .body(Some("not allowed".to_string()))
                .unwrap())
        }
    }

    #[test]
    fn test_filter_accept() {
        let filter = AcceptFilter;
        let request = http::Request::builder()
            .uri("ws://example.com/ws")
            .header("Authorization", "Bearer test")
            .body(())
            .unwrap();
        let result = filter.filter(&request);
        assert!(result.is_ok());
    }

    #[test]
    fn test_filter_reject() {
        let filter = RejectFilter;
        let request = http::Request::builder()
            .uri("ws://example.com/ws")
            .body(())
            .unwrap();
        let result = filter.filter(&request);
        assert!(result.is_err());
        let err_response = result.unwrap_err();
        assert_eq!(err_response.status(), http::StatusCode::UNAUTHORIZED);
        assert_eq!(err_response.body(), &Some("not allowed".to_string()));
    }
}
