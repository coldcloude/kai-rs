use async_trait::async_trait;

#[async_trait]
pub trait FileAppendWriter<K, R>: Send + Sync + 'static {
    async fn write(&self, key: &K, records: Vec<R>) -> crate::error::Result<()>;
}

pub trait ErrorHandler<K, R>: Send + Sync + 'static {
    fn on_write_error(&self, key: &K, batch: Vec<R>, error: &crate::error::Error);
}

pub struct NoopErrorHandler;

impl<K, R> ErrorHandler<K, R> for NoopErrorHandler {
    fn on_write_error(&self, _key: &K, _batch: Vec<R>, _error: &crate::error::Error) {}
}

use std::sync::Arc;
use std::time::Duration;
use dashmap::DashMap;
use tokio::sync::{mpsc, Notify};

pub struct FileObjectAppender<K, R, W, E = NoopErrorHandler> {
    writer: Arc<W>,
    error_handler: Arc<E>,
    map: Arc<DashMap<K, (mpsc::UnboundedSender<Vec<R>>, Arc<Notify>)>>,
    timeout: Duration,
    batch_size: usize,
}

impl<K, R, W, E> FileObjectAppender<K, R, W, E>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    R: Clone + Send + 'static,
    W: FileAppendWriter<K, R>,
    E: ErrorHandler<K, R>,
{
    pub fn new(writer: W, error_handler: E, timeout: Duration, batch_size: usize) -> Self {
        Self {
            writer: Arc::new(writer),
            error_handler: Arc::new(error_handler),
            map: Arc::new(DashMap::new()),
            timeout,
            batch_size,
        }
    }

    fn get_or_create_sender(&self, key: &K) -> mpsc::UnboundedSender<Vec<R>> {
        use dashmap::mapref::entry::Entry;
        match self.map.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get().0.clone(),
            Entry::Vacant(entry) => {
                let (tx, rx) = mpsc::unbounded_channel();
                let notify = Arc::new(Notify::new());
                let tx_for_map = tx.clone();
                entry.insert((tx_for_map, notify.clone()));
                let key_for_task = key.clone();
                let map = self.map.clone();
                let writer = self.writer.clone();
                let error_handler = self.error_handler.clone();
                let timeout = self.timeout;
                let batch_size = self.batch_size;
                tokio::spawn(Self::flush_task(
                    key_for_task, tx.clone(), rx, notify, map, writer, error_handler, timeout, batch_size,
                ));
                tx
            }
        }
    }

    pub async fn append(&self, key: K, records: Vec<R>) {
        if records.is_empty() {
            return;
        }
        let mut records = records;
        loop {
            let sender = self.get_or_create_sender(&key);
            match sender.send(records) {
                Ok(()) => return,
                Err(e) => records = e.0,
            }
        }
    }

    pub async fn flush_key(&self, key: &K) {
        if let Some(entry) = self.map.get(key) {
            entry.value().1.notify_one();
        }
    }

    pub async fn flush_all(&self) {
        for entry in self.map.iter() {
            entry.value().1.notify_one();
        }
    }
}

impl<K, R, W, E> FileObjectAppender<K, R, W, E>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    R: Clone + Send + 'static,
    W: FileAppendWriter<K, R>,
    E: ErrorHandler<K, R>,
{
    #[allow(clippy::too_many_arguments)]
    async fn flush_task(
        key: K,
        tx: mpsc::UnboundedSender<Vec<R>>,
        mut rx: mpsc::UnboundedReceiver<Vec<R>>,
        notify: Arc<Notify>,
        map: Arc<DashMap<K, (mpsc::UnboundedSender<Vec<R>>, Arc<Notify>)>>,
        writer: Arc<W>,
        error_handler: Arc<E>,
        timeout: Duration,
        batch_size: usize,
    ) {
        let mut batch = Vec::with_capacity(batch_size);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            tokio::select! {
                Some(chunk) = rx.recv() => {
                    batch.extend(chunk);
                    if batch.len() >= batch_size {
                        break;
                    }
                }
                _ = tokio::time::sleep(remaining) => break,
                _ = notify.notified() => break,
            }
        }

        map.remove(&key);
        drop(tx);

        while let Ok(chunk) = rx.try_recv() {
            batch.extend(chunk);
        }
        if !batch.is_empty() {
            if let Err(e) = writer.write(&key, batch.clone()).await {
                error_handler.on_write_error(&key, batch, &e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockWriter {
        writes: Arc<Mutex<Vec<(String, Vec<String>)>>>,
    }
    #[async_trait]
    impl FileAppendWriter<String, String> for MockWriter {
        async fn write(&self, key: &String, records: Vec<String>) -> crate::error::Result<()> {
            self.writes.lock().unwrap().push((key.clone(), records));
            Ok(())
        }
    }

    #[allow(dead_code)]
    struct MockErrorHandler {
        errors: Arc<Mutex<Vec<(String, Vec<String>, String)>>>,
    }
    impl ErrorHandler<String, String> for MockErrorHandler {
        fn on_write_error(&self, key: &String, batch: Vec<String>, error: &crate::error::Error) {
            self.errors.lock().unwrap().push((key.clone(), batch, error.to_string()));
        }
    }

    #[tokio::test]
    async fn test_append_creates_sender_for_new_key() {
        let writer = MockWriter { writes: Arc::new(Mutex::new(Vec::new())) };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        assert!(appender.map.contains_key("key1"));
    }

    #[tokio::test]
    async fn test_append_empty_vec_is_noop() {
        let writer = MockWriter { writes: Arc::new(Mutex::new(Vec::new())) };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec![]).await;
        assert!(!appender.map.contains_key("key1"));
    }

    #[tokio::test]
    async fn test_append_reuses_existing_sender() {
        let writer = MockWriter { writes: Arc::new(Mutex::new(Vec::new())) };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        let entry_count = appender.map.len();
        appender.append("key1".to_string(), vec!["b".to_string()]).await;
        assert_eq!(appender.map.len(), entry_count);
    }

    #[tokio::test]
    async fn test_flush_triggers_on_batch_size() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let writer = MockWriter { writes: writes.clone() };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_secs(60), 2);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        appender.append("key1".to_string(), vec!["b".to_string()]).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let writes = writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, "key1");
        assert_eq!(writes[0].1.len(), 2);
    }

    #[tokio::test]
    async fn test_flush_triggers_on_timeout() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let writer = MockWriter { writes: writes.clone() };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let writes = writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].1, vec!["a".to_string()]);
    }

    #[tokio::test]
    async fn test_error_handler_called_on_write_failure() {
        struct FailingWriter;
        #[async_trait]
        impl FileAppendWriter<String, String> for FailingWriter {
            async fn write(&self, _key: &String, _records: Vec<String>) -> crate::error::Result<()> {
                Err(crate::error::Error::ExternalError(
                    std::io::Error::new(std::io::ErrorKind::Other, "write failed").into(),
                ))
            }
        }

        let errors = Arc::new(Mutex::new(Vec::new()));
        let handler = MockErrorHandler { errors: errors.clone() };
        let appender: FileObjectAppender<String, String, FailingWriter, MockErrorHandler> =
            FileObjectAppender::new(FailingWriter, handler, Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let errors = errors.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "key1");
    }

    #[tokio::test]
    async fn test_new_channel_created_after_flush() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let writer = MockWriter { writes: writes.clone() };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(!appender.map.contains_key("key1"));

        appender.append("key1".to_string(), vec!["b".to_string()]).await;
        assert!(appender.map.contains_key("key1"));
    }

    #[tokio::test]
    async fn test_flush_key_triggers_write() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let writer = MockWriter { writes: writes.clone() };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_secs(60), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        appender.flush_key(&"key1".to_string()).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let writes = writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].1, vec!["a".to_string()]);
    }

    #[tokio::test]
    async fn test_flush_all_triggers_all_keys() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let writer = MockWriter { writes: writes.clone() };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_secs(60), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        appender.append("key2".to_string(), vec!["b".to_string()]).await;
        appender.flush_all().await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let writes = writes.lock().unwrap();
        assert_eq!(writes.len(), 2);
    }

    #[tokio::test]
    async fn test_flush_key_nonexistent_key_is_noop() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let writer = MockWriter { writes: writes.clone() };
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_secs(60), 100);

        appender.flush_key(&"nonexistent".to_string()).await;
        // No panic, no writes
    }
}
