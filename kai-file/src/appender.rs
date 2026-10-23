use async_trait::async_trait;

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicUsize};
use std::time::Duration;
use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};

use crate::error::{Error, Result};

#[async_trait]
pub trait FileAppendWriter<K, R, C>: Send + Sync + 'static
where
    C: FileAppendWriterContext<K, R>,
{
    async fn get_lock(&self, key: &K) -> Arc<Mutex<C>>;
    async fn remove_lock(&self, key: &K);
}

#[async_trait]
pub trait FileAppendWriterContext<K, R>: Send + Sync + 'static {
    async fn write(&mut self, key: &K, mut records: Vec<R>) -> Result<()>;
}

#[async_trait]
pub trait ErrorHandler<K, R>: Send + Sync + 'static {
    async fn on_write_error(&self, key: &K, batch: Vec<R>, error: &Error);
}

pub struct NoopErrorHandler;

#[async_trait]
impl<K, R> ErrorHandler<K, R> for NoopErrorHandler
where
    K: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    async fn on_write_error(&self, _key: &K, _batch: Vec<R>, _error: &Error) {}
}

struct FileObjectChunk<R> {
    records: Vec<R>,
    promise: oneshot::Sender<Result<()>>
}

struct FileObjectBuffer<R> {
    batch_size: usize,
    sender: mpsc::UnboundedSender<FileObjectChunk<R>>,
    notify: Arc<Notify>,
    count: AtomicUsize,
}

impl<R> FileObjectBuffer<R> {
    fn send(&self, records: Vec<R>) -> std::result::Result<oneshot::Receiver<Result<()>>, Vec<R>> {
        let size = records.len();
        let (promise_tx, promise_rx) = oneshot::channel();
        if let Err(e) = self.sender.send(FileObjectChunk {
            records,
            promise: promise_tx,
        }) {
            return Err(e.0.records);
        }
        let base = self.count.fetch_add(size, Ordering::Relaxed);
        if base + size >= self.batch_size {
            self.notify.notify_one();
        }
        Ok(promise_rx)
    }
}

pub struct FileObjectAppender<K, R, W, C, E = NoopErrorHandler> {
    _marker: PhantomData<C>,
    writer: Arc<W>,
    error_handler: Arc<E>,
    map: Arc<DashMap<K, Arc<FileObjectBuffer<R>>>>,
    timeout: Duration,
    batch_size: usize,
}

impl<K, R, W, C, E> FileObjectAppender<K, R, W, C, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    W: FileAppendWriter<K, R, C>,
    C: FileAppendWriterContext<K, R>,
    E: ErrorHandler<K, R>,
{
    pub fn new(writer: Arc<W>, error_handler: Arc<E>, timeout: Duration, batch_size: usize) -> Self {
        Self {
            _marker: PhantomData,
            writer,
            error_handler,
            map: Arc::new(DashMap::new()),
            timeout,
            batch_size,
        }
    }

    fn get_or_create_sender(&self, key: &K) -> Arc<FileObjectBuffer<R>> {
        match self.map.entry(key.clone()) {
            dashmap::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::Entry::Vacant(entry) => {
                // create buffer
                let (tx, mut rx) = mpsc::unbounded_channel();
                let buffer = Arc::new(FileObjectBuffer {
                    batch_size: self.batch_size,
                    sender: tx.clone(),
                    notify: Arc::new(Notify::new()),
                    count: AtomicUsize::new(0),
                });
                entry.insert(buffer.clone());

                // start flush task
                let key_for_task = key.clone();
                let buffer_for_task = buffer.clone();
                let map = self.map.clone();
                let writer = self.writer.clone();
                let error_handler = self.error_handler.clone();
                let timeout = self.timeout;
                tokio::spawn(async move {
                    // wait for data or timeout
                    tokio::select! {
                        _ = tokio::time::sleep(timeout) => {},
                        _ = buffer_for_task.notify.notified() => {},
                    }

                    {
                        // lock and write file
                        let lock = writer.get_lock(&key_for_task).await;
                        let mut ctx = lock.lock().await;

                        map.remove(&key_for_task);
                        rx.close();
                        let mut lists = Vec::new();
                        while let Some(chunk) = rx.recv().await {
                            lists.push(chunk);
                        }
                        let mut size = 0 as usize;
                        for chunk in lists.iter() {
                            size += chunk.records.len();
                        }
                        let mut batch = Vec::with_capacity(size);
                        let mut promises = Vec::with_capacity(lists.len());
                        for chunk in lists {
                            batch.extend(chunk.records);
                            promises.push(chunk.promise);
                        }
                        let mut result = Ok(());
                        if !batch.is_empty() {
                            if let Err(e) = ctx.write(&key_for_task, batch.clone()).await {
                                error_handler.on_write_error(&key_for_task, batch, &e).await;
                                result = Err(e);
                            }
                        }
                        for promise in promises {
                            let r = match &result {
                                Ok(_) => Ok(()),
                                Err(e) => Err(Error::WriteError(e.to_string())),
                            };
                            let _ = promise.send(r);
                        }
                    }

                    writer.remove_lock(&key_for_task).await;
                });

                buffer
            }
        }
    }

    pub async fn append(&self, key: K, records: Vec<R>) -> oneshot::Receiver<Result<()>>{
        if records.is_empty() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Ok(()));
            return rx;
        }
        let mut records = records;
        loop {
            let buffer = self.get_or_create_sender(&key);
            match buffer.send(records) {
                Ok(rx) => return rx,
                Err(r) => records = r,
            }
        }
    }

    pub async fn get_lock(&self, key: &K) -> Arc<Mutex<C>> {
        self.writer.get_lock(key).await
    }

    pub async fn remove_lock(&self, key: &K) {
        self.writer.remove_lock(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockWriterContext {
        buffer: Vec<(String, Vec<String>)>,
    }
    #[async_trait]
    impl FileAppendWriterContext<String, String> for MockWriterContext {
        async fn write(&mut self, key: &String, records: Vec<String>) -> Result<()> {
            self.buffer.push((key.clone(), records));
            Ok(())
        }
    }

    struct MockWriter {
        context: Arc<Mutex<MockWriterContext>>,
        /// 记录 remove_lock 的调用，用于断言落盘后锁确实被回收
        removed: Arc<Mutex<Vec<String>>>,
    }
    impl MockWriter {
        fn new() -> Self {
            Self {
                context: Arc::new(Mutex::new(MockWriterContext {
                    buffer: Vec::new()
                })),
                removed: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }
    #[async_trait]
    impl FileAppendWriter<String, String, MockWriterContext> for MockWriter {
        async fn get_lock(&self, _key: &String) -> Arc<Mutex<MockWriterContext>> {
            self.context.clone()
        }
        async fn remove_lock(&self, key: &String) {
            self.removed.lock().await.push(key.clone());
        }
    }

    /// 写盘必定失败的上下文，用于验证失败路径的承诺兑现与锁回收
    struct FailingWriterContext;
    #[async_trait]
    impl FileAppendWriterContext<String, String> for FailingWriterContext {
        async fn write(&mut self, _key: &String, _records: Vec<String>) -> Result<()> {
            Err(Error::IoError(std::io::Error::other("write failed")))
        }
    }

    struct FailingWriter {
        removed: Arc<Mutex<Vec<String>>>,
    }
    impl FailingWriter {
        fn new() -> Self {
            Self {
                removed: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }
    #[async_trait]
    impl FileAppendWriter<String, String, FailingWriterContext> for FailingWriter {
        async fn get_lock(&self, _key: &String) -> Arc<Mutex<FailingWriterContext>> {
            Arc::new(Mutex::new(FailingWriterContext))
        }
        async fn remove_lock(&self, key: &String) {
            self.removed.lock().await.push(key.clone());
        }
    }
    #[allow(dead_code)]
    struct MockErrorHandler {
        errors: Arc<Mutex<Vec<(String, Vec<String>, String)>>>,
    }
    #[async_trait]
    impl ErrorHandler<String, String> for MockErrorHandler {
        async fn on_write_error(&self, key: &String, batch: Vec<String>, error: &Error) {
            self.errors.lock().await.push((key.clone(), batch, error.to_string()));
        }
    }

    #[tokio::test]
    async fn test_append_creates_sender_for_new_key() {
        let writer = Arc::new(MockWriter::new());
        let appender = FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        assert!(appender.map.contains_key("key1"));
    }

    #[tokio::test]
    async fn test_append_empty_vec_is_noop() {
        let writer = Arc::new(MockWriter::new());
        let appender = FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec![]).await;
        assert!(!appender.map.contains_key("key1"));
    }

    #[tokio::test]
    async fn test_append_reuses_existing_sender() {
        let writer = Arc::new(MockWriter::new());
        let appender = FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        let entry_count = appender.map.len();
        appender.append("key1".to_string(), vec!["b".to_string()]).await;
        assert_eq!(appender.map.len(), entry_count);
    }

    #[tokio::test]
    async fn test_flush_triggers_on_batch_size() {
        let writer = Arc::new(MockWriter::new());
        let appender = FileObjectAppender::new(writer.clone(), Arc::new(NoopErrorHandler {}), Duration::from_secs(60), 2);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        appender.append("key1".to_string(), vec!["b".to_string()]).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let writes = writer.context.lock().await;
        assert_eq!(writes.buffer.len(), 1);
        assert_eq!(writes.buffer[0].0, "key1");
        assert_eq!(writes.buffer[0].1.len(), 2);
    }

    #[tokio::test]
    async fn test_flush_triggers_on_timeout() {
        let writer = Arc::new(MockWriter::new());
        let appender = FileObjectAppender::new(writer.clone(), Arc::new(NoopErrorHandler {}), Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let writes = writer.context.lock().await;
        assert_eq!(writes.buffer.len(), 1);
        assert_eq!(writes.buffer[0].1, vec!["a".to_string()]);
    }

    #[tokio::test]
    async fn test_error_handler_called_on_write_failure() {
        let errors = Arc::new(Mutex::new(Vec::new()));
        let handler = Arc::new(MockErrorHandler { errors: errors.clone() });
        let appender = FileObjectAppender::new(Arc::new(FailingWriter::new()), handler, Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let errors = errors.lock().await;
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "key1");
    }

    #[tokio::test]
    async fn test_new_channel_created_after_flush() {
        let writer = Arc::new(MockWriter::new());
        let appender = FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(!appender.map.contains_key("key1"));

        appender.append("key1".to_string(), vec!["b".to_string()]).await;
        assert!(appender.map.contains_key("key1"));
    }

    /// 同一批次内的记录顺序必须与 append 调用顺序一致
    #[tokio::test]
    async fn test_batch_preserves_append_order() {
        let writer = Arc::new(MockWriter::new());
        // batch_size 设大，确保三个 chunk 只能靠 timeout 触发、落入同一批次
        let appender = FileObjectAppender::new(writer.clone(), Arc::new(NoopErrorHandler {}), Duration::from_millis(100), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        appender.append("key1".to_string(), vec!["b".to_string()]).await;
        appender.append("key1".to_string(), vec!["c".to_string()]).await;

        tokio::time::sleep(Duration::from_millis(400)).await;

        let writes = writer.context.lock().await;
        assert_eq!(writes.buffer.len(), 1);
        assert_eq!(
            writes.buffer[0].1,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    /// append 返回的承诺在落盘成功后兑现 Ok
    #[tokio::test]
    async fn test_promise_resolved_ok_after_flush() {
        let writer = Arc::new(MockWriter::new());
        let appender = FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_millis(50), 100);

        let rx = appender.append("key1".to_string(), vec!["a".to_string()]).await;

        let result = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("承诺未在超时内兑现")
            .expect("承诺发送端被丢弃");
        assert!(result.is_ok());
    }

    /// 落盘成功后必须回收该 key 的锁
    #[tokio::test]
    async fn test_remove_lock_called_after_success_flush() {
        let writer = Arc::new(MockWriter::new());
        let removed = writer.removed.clone();
        let appender = FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(*removed.lock().await, vec!["key1".to_string()]);
    }

    /// 落盘失败时承诺兑现 Err，且锁仍必须被回收
    /// （回归：错误路径曾直接 return，跳过了 remove_lock）
    #[tokio::test]
    async fn test_promise_resolved_err_and_lock_removed_on_failure() {
        let writer = Arc::new(FailingWriter::new());
        let removed = writer.removed.clone();
        let appender = FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_millis(50), 100);

        let rx = appender.append("key1".to_string(), vec!["a".to_string()]).await;

        let result = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("承诺未在超时内兑现")
            .expect("承诺发送端被丢弃");
        assert!(result.is_err());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(*removed.lock().await, vec!["key1".to_string()]);
    }

    /// 落盘失败时同一批次内所有承诺都兑现 Err，不能一部分 Ok
    #[tokio::test]
    async fn test_all_promises_resolved_err_on_failure() {
        let writer = Arc::new(FailingWriter::new());
        let appender = FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_millis(100), 100);

        let rx1 = appender.append("key1".to_string(), vec!["a".to_string()]).await;
        let rx2 = appender.append("key1".to_string(), vec!["b".to_string()]).await;

        let r1 = tokio::time::timeout(Duration::from_secs(2), rx1)
            .await
            .expect("承诺1未在超时内兑现")
            .expect("承诺1发送端被丢弃");
        let r2 = tokio::time::timeout(Duration::from_secs(2), rx2)
            .await
            .expect("承诺2未在超时内兑现")
            .expect("承诺2发送端被丢弃");
        assert!(r1.is_err());
        assert!(r2.is_err());
    }
}
