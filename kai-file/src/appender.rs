use async_trait::async_trait;

use std::collections::{LinkedList};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicUsize};
use std::time::Duration;
use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, mpsc};

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
    async fn write(&mut self, key: &K, records: Vec<R>) -> Result<()>;
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

struct FileObjectBuffer<R> {
    batch_size: usize,
    sender: mpsc::UnboundedSender<Vec<R>>,
    notify: Arc<Notify>,
    count: AtomicUsize,
}

impl<R> FileObjectBuffer<R> {
    fn send(&self, records: Vec<R>) -> std::result::Result<(), mpsc::error::SendError<Vec<R>>> {
        let size = records.len();
        self.sender.send(records)?;
        let base = self.count.fetch_add(size, Ordering::Relaxed);
        if base + size >= self.batch_size {
            self.notify.notify_one();
        }
        Ok(())
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
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(timeout) => break,
                            _ = buffer_for_task.notify.notified() => break,
                        }
                    }

                    {
                        // lock and write file
                        let lock = writer.get_lock(&key_for_task).await;
                        let mut ctx = lock.lock().await;

                        map.remove(&key_for_task);
                        rx.close();
                        let mut lists = LinkedList::new();
                        while let Some(chunk) = rx.recv().await {
                            lists.push_back(chunk);
                        }
                        let mut size = 0 as usize;
                        for chunk in lists.iter() {
                            size += chunk.len();
                        }
                        let mut batch = Vec::with_capacity(size);
                        while let Some(chunk) = lists.pop_back() {
                            batch.extend(chunk);
                        }
                        if !batch.is_empty() {
                            if let Err(e) = ctx.write(&key_for_task, batch.clone()).await {
                                error_handler.on_write_error(&key_for_task, batch, &e).await;
                            }
                        }
                    }

                    writer.remove_lock(&key_for_task).await;
                });

                buffer
            }
        }
    }

    pub async fn append(&self, key: K, records: Vec<R>) {
        if records.is_empty() {
            return;
        }
        let mut records = records;
        loop {
            let buffer = self.get_or_create_sender(&key);
            match buffer.send(records) {
                Ok(()) => return,
                Err(e) => records = e.0,
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
    }
    impl MockWriter {
        fn new() -> Self {
            Self {
                context: Arc::new(Mutex::new(MockWriterContext {
                    buffer: Vec::new()
                })),
            }
        }
    }
    #[async_trait]
    impl FileAppendWriter<String, String, MockWriterContext> for MockWriter {
        async fn get_lock(&self, _key: &String) -> Arc<Mutex<MockWriterContext>> {
            self.context.clone()
        }
        async fn remove_lock(&self, _key: &String) {
            // No-op
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
        struct FailingWriterContext;
        #[async_trait]
        impl FileAppendWriterContext<String, String> for FailingWriterContext {
            async fn write(&mut self, _key: &String, _records: Vec<String>) -> Result<()> {
                Err(Error::IoError(
                    std::io::Error::new(std::io::ErrorKind::Other, "write failed").into(),
                ))
            }
        }
        struct FailingWriter;
        #[async_trait]
        impl FileAppendWriter<String, String, FailingWriterContext> for FailingWriter {
            async fn get_lock(&self, _key: &String) -> Arc<Mutex<FailingWriterContext>> {
                Arc::new(Mutex::new(FailingWriterContext))
            }
            async fn remove_lock(&self, _key: &String) {
                // No-op
            }
        }

        let errors = Arc::new(Mutex::new(Vec::new()));
        let handler = Arc::new(MockErrorHandler { errors: errors.clone() });
        let appender = FileObjectAppender::new(Arc::new(FailingWriter {}), handler, Duration::from_millis(50), 100);

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
}
