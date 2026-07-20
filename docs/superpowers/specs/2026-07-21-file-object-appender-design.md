# FileObjectAppender 设计文档

## 概述

在 kai-file 模块中新增 `FileObjectAppender<K, R, W, E>`，用于向文件中按结构体批量缓冲写入数据。每个 key 对应一个文件，数据以 Vec<R> 原子入队，由后台异步任务按 batch_size / timeout 触发 flush 写入。

## 核心 Trait

```rust
#[async_trait]
pub trait FileAppendWriter<K, R>: Send + Sync + 'static {
    async fn write(&self, key: &K, records: Vec<R>) -> Result<()>;
}

pub trait ErrorHandler<K, R>: Send + Sync + 'static {
    fn on_write_error(&self, key: &K, batch: Vec<R>, error: &crate::error::Error);
}
```

- `FileAppendWriter` 定义实际写入逻辑，接收 key + 一批记录
- `ErrorHandler` 定义写入失败后的回调（处理完毕后不重试）

## 类型定义

```rust
pub struct FileObjectAppender<K, R, W, E = NoopErrorHandler> {
    writer: Arc<W>,
    error_handler: Arc<E>,
    map: DashMap<K, (mpsc::UnboundedSender<Vec<R>>, Arc<Notify>)>,
    timeout: Duration,
    batch_size: usize,
}
```

- `K`: `Eq + Hash + Clone + Send + Sync + 'static`
- `R`: `Send + 'static`
- `W`: `FileAppendWriter<K, R>`
- `E`: `ErrorHandler<K, R>`（默认 NoopErrorHandler，空实现）

## 构造

```rust
impl<K, R, W, E> FileObjectAppender<K, R, W, E> {
    pub fn new(writer: W, error_handler: E, timeout: Duration, batch_size: usize) -> Self
}
```

## 公开方法

```rust
/// 原子入队一批记录，不触发 flush
pub async fn append(&self, key: K, records: Vec<R>) -> Result<()>;

/// 主动触发单个 key 的 flush
pub async fn flush_key(&self, key: &K);

/// 主动触发所有 key 的 flush
pub async fn flush_all(&self);
```

## append 路径

```
append(key, records):
  if records.is_empty() → return

  loop:
    sender = get_or_create_sender(key)
    match sender.send(records):
      Ok(()) → return
      Err(SendError(r)) → records = r（所有权返回）, continue
```

## get_or_create_sender

```
get_or_create_sender(key):
  match map.entry(key.clone()):
    Occupied(entry) → return entry.get().0.clone()
    Vacant → 创建 unbounded_channel()
             创建 Notify
             存 (sender, notify) 到 map
             spawn flush_task(key, tx_clone, rx, notify_clone, ...)
             return sender
```

## flush_task（单次后台任务）

```
flush_task(key, tx_for_close, rx, notify, map, writer, error_handler, timeout, batch_size):
  let mut batch = Vec::with_capacity(batch_size)
  let deadline = Instant::now() + timeout

  loop:
    remaining = deadline - now()
    if remaining == 0 → break
    select!:
      Some(chunk) = rx.recv():
        batch.extend(chunk)
        if batch.len() >= batch_size → break
      _ = sleep(remaining) → break
      _ = notify.notified() → break   // flush_key 主动触发

  map.remove(&key)
  tx_for_close.close()

  if !batch.is_empty():
    while let Ok(chunk) = rx.try_recv():
      batch.extend(chunk)
    if let Err(e) = writer.write(&key, batch).await:
      error_handler.on_write_error(&key, batch, &e)

  // 退出 → drop tx_for_close → drop rx → channel 关闭
```

## flush_key / flush_all

map 中每个 entry 同时存储 `Arc<Notify>`，flush task 在 select 循环中监听 notify。调用 `flush_key` 时触发 notify，task 收到后立即 break 进入 write 阶段。

```
flush_key(key):
  map.get(key)?.1.notify_one()     // 触发通知

flush_all():
  for entry in map.iter():
    entry.value().1.notify_one()   // 逐个触发
```

## 状态流转图

```
append(key, records)
  │
  ├─ map 无 entry → 创建 UnboundedChannel
  │                  map.insert(key, tx)
  │                  spawn flush_task
  │                  send(records) → Ok
  │
  └─ map 有 entry → send(records)
                      ├─ Ok → 完成
                      └─ SendError → 重试（所有权返回）

flush_task:
  ┌─ 收集到 batch_size / timeout
  ├─ map.remove(&key)
  ├─ tx.close()
  ├─ drain rx（try_recv）
  ├─ writer.write(&key, batch)
  │   └─ 失败 → error_handler.on_write_error(...)
  └─ 退出
```

## 边界情况

- **空 Vec 入队**: `append` 立即返回，不创建 channel 或 task
- **timeout 先于首批消息**: 循环 break，batch 为空，不调用 write，直接退出
- **写入失败**: 调用 error_handler 后结束，不重试，对应 key 的数据已全部写入 handler
- **并发 append 同一 key**: 无锁，UnboundedSender 线程安全，原子入队
- **flush task 期间新 append**: 此时 map 已 remove entry，新 append 创建新 channel + 新 task，两个通道独立运作

## 依赖

在现有 `Cargo.toml` 基础上无需新增依赖（tokio, dashmap, async-trait 已有）。
