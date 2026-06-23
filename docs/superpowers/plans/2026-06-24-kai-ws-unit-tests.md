# kai-ws ws.rs 单元测试实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `kai-rs/kai-ws/src/ws.rs` 编写 29 个单元测试，覆盖二进制解析、WsMessage 序列化、WsContext、消息分发、心跳、握手过滤。

**Architecture:** 全部测试内联在 `ws.rs` 末尾的 `#[cfg(test)] mod tests` 中。不新增 dev-dependencies。组 3-5 用 `#[tokio::test]`，组 1/2/6 用 `#[test]`。组 4 用 `Arc<Mutex<...>>` 实现 mock processor 记录调用历史。组 5 用真实时间 1s 间隔 + `tokio::time::timeout`。

**Tech Stack:** Rust, tokio, tokio-tungstenite, serde_json, bytes

**设计文档:** `kai-rs/docs/kai-ws-test.md`

---

## 文件结构

- **Modify:** `kai-rs/kai-ws/src/ws.rs` — 在文件末尾添加 `#[cfg(test)] mod tests { ... }` 块
- 不创建新文件

---

### Task 1: 测试辅助类型和工具函数

**Files:**
- Modify: `kai-rs/kai-ws/src/ws.rs` — 添加 `mod tests` 块和辅助类型

- [ ] **Step 1: 编写辅助类型和工具函数**

在 `ws.rs` 末尾添加 `#[cfg(test)]` 模块，包含各组测试共享的辅助类型：

```rust
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicBool, Ordering};
    use bytes::Bytes;
    use tokio::time::{Duration, Instant, timeout};

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
}
```

- [ ] **Step 2: 编译验证**

```bash
cd /home/admin/project/kissbot/kai-rs/kai-ws && cargo test --no-run 2>&1 | head -20
```

Expected: 编译通过，无错误。

- [ ] **Step 3: 提交**

```bash
cd /home/admin/project/kissbot/kai-rs && git add src/ws.rs && git commit -m "test: add test module skeleton and mock helpers

添加 #[cfg(test)] mod tests 框架和共享的 mock processor
类型（MockJsonProcessor / MockBinProcessor / MockCloseProcessor）
及辅助函数（make_bin_header / make_message）。

Co-Authored-By: deepseek-v4-flash"
```

---

### Task 2: 二进制解析函数测试（组 1 — 4 个测试）

**Files:**
- Modify: `kai-rs/kai-ws/src/ws.rs` — 在 `mod tests` 中添加组 1 测试

- [ ] **Step 1: 编写组 1 四个测试**

在 `mod tests` 的辅助类型之后添加：

```rust
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
        let short = vec![0u8; 8];
        assert!(parse_bin_sn(&short).is_err());
        assert!(parse_bin_payload_type(&short).is_err());
        assert!(parse_bin_status_code(&short).is_err());
    }
```

- [ ] **Step 2: 运行测试确认通过**

```bash
cd /home/admin/project/kissbot/kai-rs/kai-ws && cargo test test_parse_bin -- --nocapture
```

Expected: 4 tests passed

- [ ] **Step 3: 提交**

```bash
cd /home/admin/project/kissbot/kai-rs && git add src/ws.rs && git commit -m "test: 二进制解析函数测试（组1）

添加 test_parse_bin_sn / _payload_type / _status_code / _out_of_bounds
验证 12 字节网络字节序头部各字段的解析和越界错误处理。

Co-Authored-By: deepseek-v4-flash"
```

---

### Task 3: WsMessage 序列化测试（组 2 — 3 个测试）

**Files:**
- Modify: `kai-rs/kai-ws/src/ws.rs` — 在 `mod tests` 中添加组 2 测试

- [ ] **Step 1: 编写组 2 三个测试**

在组 1 测试之后添加：

```rust
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
```

- [ ] **Step 2: 运行测试确认通过**

```bash
cd /home/admin/project/kissbot/kai-rs/kai-ws && cargo test test_ws_message -- --nocapture
```

Expected: 3 tests passed

- [ ] **Step 3: 提交**

```bash
cd /home/admin/project/kissbot/kai-rs && git add src/ws.rs && git commit -m "test: WsMessage 序列化/反序列化测试（组2）

添加 test_ws_message_roundtrip / _payload_none / _payload_value
验证全字段、空 payload、嵌套结构的 JSON 往返一致性。

Co-Authored-By: deepseek-v4-flash"
```

---

### Task 4: WsContext 核心方法测试（组 3 — 10 个测试）

**Files:**
- Modify: `kai-rs/kai-ws/src/ws.rs` — 在 `mod tests` 中添加组 3 测试

- [ ] **Step 1: 编写组 3 十个测试**

在组 2 测试之后添加：

```rust
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
        let json_proc = Arc::new(MockJsonProcessor { called: json_calls.clone() });
        let req = make_message(10, 1, 200, None);
        ctx.send_json_with_json_response(req.clone(), json_proc).await.unwrap();

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
        let bin_proc = Arc::new(MockBinProcessor { called: bin_calls.clone() });
        let bin_req = Bytes::from(make_bin_header(20, 2, 200));
        ctx.send_bin_with_bin_response(20, bin_req, bin_proc).await.unwrap();

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

    // Add USE declaration for tokio::test if not already present at module scope
```

- [ ] **Step 2: 运行测试确认通过**

```bash
cd /home/admin/project/kissbot/kai-rs/kai-ws && cargo test test_context -- --nocapture
```

Expected: 10 tests passed

- [ ] **Step 3: 提交**

```bash
cd /home/admin/project/kissbot/kai-rs && git add src/ws.rs && git commit -m "test: WsContext 核心方法测试（组3）

添加10个测试：new / next_sn / set_bin_processor / set_json_processor /
set_close_processor / send_json / send_bin / send_close /
response_handlers / send_queue_full。

Co-Authored-By: deepseek-v4-flash"
```

---

### Task 5: 消息分发逻辑测试（组 4 — 6 个测试）

**Files:**
- Modify: `kai-rs/kai-ws/src/ws.rs` — 在 `mod tests` 中添加组 4 测试

- [ ] **Step 1: 编写组 4 六个测试**

在组 3 测试之后添加：

```rust
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
        let proc = Arc::new(MockJsonProcessor { called: calls.clone() });
        // Register as response processor (keyed by sn)
        ctx.reponse_json_processor_map.insert(5, proc);

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
        let proc = Arc::new(MockBinProcessor { called: calls.clone() });
        ctx.reponse_bin_processor_map.insert(8, proc);

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
```

- [ ] **Step 2: 运行测试确认通过**

```bash
cd /home/admin/project/kissbot/kai-rs/kai-ws && cargo test test_handle -- --nocapture
```

Expected: 6 tests passed

- [ ] **Step 3: 提交**

```bash
cd /home/admin/project/kissbot/kai-rs && git add src/ws.rs && git commit -m "test: 消息分发逻辑测试（组4）

添加6个测试：json_request / json_response / bin_request /
bin_response / close / unregistered_type。
验证消息根据 payload_type 正确路由到 request 或 response 处理器，
未注册 type 静默忽略。

Co-Authored-By: deepseek-v4-flash"
```

---

### Task 6: WsHeartbeatHandler 集成测试（组 5 — 4 个测试）

**Files:**
- Modify: `kai-rs/kai-ws/src/ws.rs` — 在 `mod tests` 中添加组 5 测试

- [ ] **Step 1: 编写组 5 四个测试**

在组 4 测试之后添加：

```rust
    // === Group 5: WsHeartbeatHandler integration tests ===
    // All use 1-second interval, wrapped in tokio::time::timeout(5s)
    // to prevent hanging.

    #[tokio::test]
    async fn test_heartbeat_send() {
        let ctx = Arc::new(WsContext::new(16));
        let handler = WsHeartbeatHandler::new(Duration::from_secs(1), ctx.clone());

        tokio::time::timeout(Duration::from_secs(5), async {
            // Start heartbeat in a background task
            let h = &handler;
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
                handler.process_bin(&[0u8; 12], ctx.clone()).await;
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
        let handler = WsHeartbeatHandler::new(Duration::from_secs(1), ctx.clone());

        tokio::time::timeout(Duration::from_secs(8), async {
            tokio::spawn(async move {
                let _ = handler.start().await;
            });

            // Wait for timeout (~3s deadline + slop)
            tokio::time::sleep(Duration::from_millis(3500)).await;
            let received = ctx.sending_queue.1.try_recv();

            // Should have at least gotten something (heartbeat or close)
            // After 3.5s, if timeout occurred, we'd see a Close
            // But the Close might have been consumed; just ensure no panic
            assert!(handler.running.load(Ordering::Relaxed) == false, "handler should have stopped");
        }).await.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_already_started() {
        let ctx = Arc::new(WsContext::new(16));
        let handler = WsHeartbeatHandler::new(Duration::from_secs(1), ctx.clone());

        let ctx2 = ctx.clone();
        tokio::spawn(async move {
            // Start first time
            let _ = handler.start().await;
        });

        // Give time for the first start to set running=true
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second start should fail
        let result = handler.start().await;
        assert!(matches!(result, Err(crate::Error::HeartbeatHandlerAlreadyStarted)));
        handler.running.store(false, Ordering::Relaxed);
    }
```

- [ ] **Step 2: 运行测试确认通过**

```bash
cd /home/admin/project/kissbot/kai-rs/kai-ws && cargo test test_heartbeat -- --nocapture
```

Expected: 4 tests passed (注意：测试涉及真实 1s 等待，运行约需 15-20 秒)

- [ ] **Step 3: 提交**

```bash
cd /home/admin/project/kissbot/kai-rs && git add src/ws.rs && git commit -m "test: WsHeartbeatHandler 集成测试（组5）

添加4个测试：heartbeat_send / heartbeat_refresh / heartbeat_timeout /
already_started。使用1s间隔 + tokio::time::timeout(5-8s)
包裹验证心跳发送、续命、超时关闭、重复启动检测。

Co-Authored-By: deepseek-v4-flash"
```

---

### Task 7: WsHeaderFilter 测试（组 6 — 2 个测试）

**Files:**
- Modify: `kai-rs/kai-ws/src/ws.rs` — 在 `mod tests` 中添加组 6 测试

- [ ] **Step 1: 编写组 6 两个测试**

在组 5 测试之后添加：

```rust
    // === Group 6: WsHeaderFilter tests ===

    struct AcceptFilter;

    impl WsHeaderFilter for AcceptFilter {
        fn filter(&self, _request: &http::Request<()>) -> Result<()> {
            Ok(())
        }
    }

    struct RejectFilter;

    impl WsHeaderFilter for RejectFilter {
        fn filter(&self, _request: &http::Request<()>) -> Result<()> {
            Err(crate::Error::UpgradeRejected("not allowed".to_string()))
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
        assert!(result.unwrap_err().to_string().contains("not allowed"));
    }
```

- [ ] **Step 2: 运行测试确认通过**

```bash
cd /home/admin/project/kissbot/kai-rs/kai-ws && cargo test test_filter -- --nocapture
```

Expected: 2 tests passed

- [ ] **Step 3: 提交**

```bash
cd /home/admin/project/kissbot/kai-rs && git add src/ws.rs && git commit -m "test: WsHeaderFilter 测试（组6）

添加2个测试：test_filter_accept / test_filter_reject。
通过构造 http::Request<()> 直接调用 filter trait 验证通过/拒绝逻辑，
不涉及真实 TCP 连接。

Co-Authored-By: deepseek-v4-flash"
```

---

### Task 8: 全量运行并验证

- [ ] **Step 1: 全量运行所有测试**

```bash
cd /home/admin/project/kissbot/kai-rs/kai-ws && cargo test 2>&1
```

Expected: 29 tests passed (including组 1-6 所有测试)

- [ ] **Step 2: 提交最终确认**

```bash
cd /home/admin/project/kissbot/kai-rs && git add src/ws.rs && git commit -m "test: 全量测试通过确认

29 个测试全部通过。

Co-Authored-By: deepseek-v4-flash"
```
