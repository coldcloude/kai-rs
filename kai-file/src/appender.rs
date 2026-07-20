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
    map: DashMap<K, (mpsc::UnboundedSender<Vec<R>>, Arc<Notify>)>,
    timeout: Duration,
    batch_size: usize,
}

impl<K, R, W, E> FileObjectAppender<K, R, W, E>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    R: Send + 'static,
    W: FileAppendWriter<K, R>,
    E: ErrorHandler<K, R>,
{
    pub fn new(writer: W, error_handler: E, timeout: Duration, batch_size: usize) -> Self {
        Self {
            writer: Arc::new(writer),
            error_handler: Arc::new(error_handler),
            map: DashMap::new(),
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
}

impl<K, R, W, E> FileObjectAppender<K, R, W, E>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    R: Send + 'static,
    W: FileAppendWriter<K, R>,
    E: ErrorHandler<K, R>,
{
    async fn flush_task(
        key: K,
        tx: mpsc::UnboundedSender<Vec<R>>,
        rx: mpsc::UnboundedReceiver<Vec<R>>,
        notify: Arc<Notify>,
        map: DashMap<K, (mpsc::UnboundedSender<Vec<R>>, Arc<Notify>)>,
        writer: Arc<W>,
        error_handler: Arc<E>,
        timeout: Duration,
        batch_size: usize,
    ) {
        let _ = (key, tx, rx, notify, map, writer, error_handler, timeout, batch_size);
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
}
