# kai-ws WebSocket 通信模块

提供基于 tokio-tungstenite 的 WebSocket 通信框架，支持 JSON 和二进制消息收发、心跳检测、请求-响应模式、以及 WSS 握手阶段的请求头过滤。

## 功能

### 1. WebSocket 连接管理
- `ws_handle_connection` 接受 `TcpStream`，完成 WebSocket Upgrade 握手后，建立发送和接收两个异步任务
- 支持 JSON 消息（`WsMessage` 结构体：sn、payload_type、status_code、payload）
- 支持二进制消息（字节流）
- 通过 `WsContext` 管理连接上下文：序列号生成、消息发送队列、处理器注册

### 2. 消息处理
- `WsJsonProcessor` trait：处理 JSON 消息
- `WsBinaryProcessor` trait：处理二进制消息
- `WsCloseProcessor` trait：处理连接关闭事件
- 处理器通过 `WsProcessorInitializer` 在连接建立时注册
- 支持请求-响应模式（`send_json_with_json_response`、`send_bin_with_json_response` 等）

### 3. 心跳检测
- `WsHeartbeatHandler` 提供定时心跳发送和超时断开
- 可配置心跳间隔
- 在收到任何数据时刷新超时 deadline

### 4. WSS 握手过滤（v2 新增）
- `WssRequestFilter` trait：在 WebSocket Upgrade 握手阶段检查 HTTP 请求头的回调接口
- 通过 `accept_hdr_async` 替代 `accept_async`，在握手完成后的回调中暴露请求头
- filter 接收 `&http::Request`，返回 `Result<()>`：
  - `Ok(())`：握手继续，建立 WebSocket 连接
  - `Err(kai_ws::Error)`：握手拒绝，关闭连接
- `ws_handle_connection` 增加可选的 filter 参数

## 设计说明

### 消息格式
所有 JSON 消息使用统一的 `WsMessage` 结构：
- `sn`：序列号（4字节，网络字节序）
- `payload_type`：消息类型（4字节，网络字节序）
- `status_code`：状态码（4字节，网络字节序）
- `payload`：可选的 JSON 数据

二进制消息的头部同样包含 sn、payload_type、status_code。

### WSS 过滤设计原则（v2）
- kai-ws 只提供过滤接口（`WssRequestFilter` trait），不包含认证逻辑
- filter 是可选参数——不使用时不改变现有行为
- 使用 `accept_hdr_async` 替代 `accept_async`，在 tungstenite 层完成协议解析后、WebSocket 流建立前检查请求头
- 拒绝时返回自定义错误码，连接立即关闭

### 依赖
- tokio 1.x
- tokio-tungstenite 0.26
- futures-util 0.3
- http 1.x（v2 新增，用于 filter 回调的 Request 类型）

## 规划

### v1 已完成
- [x] 连接管理（accept、发送、接收）
- [x] JSON 消息处理
- [x] 二进制消息处理
- [x] 请求-响应模式
- [x] 心跳检测
- [x] 错误类型定义

### v2 待实现
- [ ] `WssRequestFilter` trait 定义
- [ ] `ws_handle_connection` 增加可选 filter 参数
- [ ] 改用 `accept_hdr_async` 以获取请求头
- [ ] filter 认证失败时关闭连接并返回自定义错误
