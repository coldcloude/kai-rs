# kai-ws ws.rs 单元测试设计

为 `kai-rs/kai-ws/src/ws.rs` 编写单元测试，覆盖二进制协议解析、消息路由、上下文管理、心跳检测、握手过滤。

## 测试文件位置

遵循项目惯例，测试内联在 `ws.rs` 末尾，`#[cfg(test)] mod tests` 块中。

## dev-dependencies

无需新增。`tokio` 已在 dependencies 中，支持 `#[tokio::test]`。

## 测试分组

### 1. 二进制解析函数

纯函数，同步 `#[test]`。

| 测试 | 说明 |
|------|------|
| `test_parse_bin_sn` | 构造 12 字节 buffer，验证 `parse_bin_sn` 正确提取前 4 字节（大端） |
| `test_parse_bin_payload_type` | 验证 `parse_bin_payload_type` 正确提取第 4-8 字节 |
| `test_parse_bin_status_code` | 验证 `parse_bin_status_code` 正确提取第 8-12 字节 |
| `test_parse_bin_out_of_bounds` | 传入不足 12 字节的 slice，验证返回 `Err` |

各字段边界值：全零、`u32::MAX`。

### 2. WsMessage 序列化/反序列化

`#[test]`。

| 测试 | 说明 |
|------|------|
| `test_ws_message_roundtrip` | 构造全字段 `WsMessage`，序列化后反序列化，验证字段一致 |
| `test_ws_message_payload_none` | `payload: None` 的序列化和反序列化 |
| `test_ws_message_payload_value` | `payload: Some(json!({...}))` 的嵌套结构 |

### 3. WsContext 核心方法

`#[tokio::test]`。

| 测试 | 说明 |
|------|------|
| `test_context_new` | `new()` 构造后，发送队列为空 |
| `test_context_next_sn` | 连续调用 `next_request_sn()` 返回 0, 1, 2... 单调递增 |
| `test_context_set_bin_processor` | 注册并验证 binary processor 可被检索 |
| `test_context_set_json_processor` | 注册并验证 json processor 可被检索 |
| `test_context_set_close_processor` | 注册 close processor，覆盖 `RwLock` 读写 |
| `test_context_send_json` | `send_json` 投递后，从 receiver 端收到对应 `WsMessageUnion::Json` |
| `test_context_send_bin` | `send_bin` 投递后，从 receiver 端收到 `WsMessageUnion::Binary` |
| `test_context_send_close` | `send_close` 投递后，从 receiver 端收到 `WsMessageUnion::Close` |
| `test_context_response_handlers` | 验证四组 `send_*_with_*_response`：handler 被注册到 response map，请求被发送 |
| `test_context_send_queue_full` | capacity=1 队列满时继续 send 行为（取决于 flume 的有界语义——send_async 会阻塞，此处用 timeout 确认阻塞不 panic） |

测试中用 `drop(sender)` 方式让接收端可正常迭代取消息。

### 4. 消息分发逻辑

`#[tokio::test]`。

通过实现简单的 mock processor（如 `MockJsonProcessor` / `MockBinProcessor`，内部用 `Arc<Mutex<Vec<captured>>>` 记录调用），验证 `ws_handle_json_message` 和 `ws_handle_bin_message` 的路由行为。

| 测试 | 说明 |
|------|------|
| `test_handle_json_request` | JSON payload_type ≠ 0，验证被路由到 request_json_processor_map |
| `test_handle_json_response` | JSON payload_type = 0 (TYPE_RESPONSE)，验证被路由到 response_json_processor_map |
| `test_handle_bin_request` | 二进制 payload_type ≠ 0，路由到 request_bin_processor_map |
| `test_handle_bin_response` | 二进制 payload_type = 0，路由到 response_bin_processor_map |
| `test_handle_close` | 注册 close processor 后调用 `ws_handle_close`，验证 processor 被触发 |
| `test_handle_unregistered_type` | 传入未注册 processor 的 payload_type，验证静默忽略（不 panic、不报错） |

### 5. WsHeartbeatHandler 集成测试

`#[tokio::test]`，interval = `Duration::from_secs(1)`，用 `tokio::time::timeout` 限制测试时长。

| 测试 | 说明 |
|------|------|
| `test_heartbeat_send` | 启动 handler，等待 ~1.1s，验证 sending_queue 收到一条心跳 binary 消息（payload_type = TYPE_HEARTBEAT） |
| `test_heartbeat_refresh` | 启动 handler，通过 `process_bin` 喂数据刷新 deadline，运行 ~3.5s 不应触发超时关闭 |
| `test_heartbeat_timeout` | 启动 handler 但不喂数据，等待 ~3.5s，验证触发 `send_close` |
| `test_heartbeat_already_started` | 调用两次 `start()`，第二次返回 `Err(HeartbeatHandlerAlreadyStarted)` |

每个测试使用独立的 `WsContext` 和 `WsHeartbeatHandler` 实例。

**超时边界**：deadline = 3 × interval，从 `start()` 内的第一次 `update_deadline()` 算起。测试等待时间取约 3.5× interval 以确保时序。

### 6. WsHeaderFilter 测试

`#[test]`。

直接构造 `http::Request<()>`，调用 filter trait 的 `filter()` 方法，不涉及真实 TCP 连接。

| 测试 | 说明 |
|------|------|
| `test_filter_accept` | 构造一个始终返回 `Ok(())` 的 filter，验证返回 `Ok(())` |
| `test_filter_reject` | 构造一个返回 `Err(...)` 的 filter，验证返回 `Err` |

## 总数统计

- 组 1: 4 个测试
- 组 2: 3 个测试
- 组 3: 10 个测试
- 组 4: 6 个测试
- 组 5: 4 个测试
- 组 6: 2 个测试
- **合计: 29 个测试用例**

## 测试执行

```bash
cd kai-rs/kai-ws
cargo test
```

确保 `cargo test` 通过且不调用外部网络或真实 WebSocket 服务。
