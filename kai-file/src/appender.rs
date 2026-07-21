use async_trait::async_trait;

use std::collections::{HashMap, LinkedList};
use std::hash::Hash;
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicUsize};
use std::time::Duration;
use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, RwLock, mpsc};

use crate::error::{Error, Result};

#[async_trait]
pub trait FileAppendWriter<K, R>: Send + Sync + 'static {
    async fn write(&self, key: &K, records: Vec<R>) -> Result<()>;
}

pub trait ErrorHandler<K, R>: Send + Sync + 'static {
    fn on_write_error(&self, key: &K, batch: Vec<R>, error: &Error);
}

pub struct NoopErrorHandler;

impl<K, R> ErrorHandler<K, R> for NoopErrorHandler {
    fn on_write_error(&self, _key: &K, _batch: Vec<R>, _error: &Error) {}
}

pub struct FileWriteLocks<K> {
    map: HashMap<K, Weak<Mutex<()>>>,
}
impl<K> FileWriteLocks<K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub async fn get_lock(&mut self, key: &K) -> Arc<Mutex<()>> {
        match self.map.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                match entry.get().upgrade() {
                    Some(lock) => {
                        lock
                    }
                    None => {
                        let lock = Arc::new(Mutex::new(()));
                        entry.insert(Arc::downgrade(&lock));
                        lock
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let lock = Arc::new(Mutex::new(()));
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
    }

    pub fn remove(&mut self, key: &K) {
        let lock = self.map.get(key);
        if let Some(entry) = lock {
            if entry.strong_count() == 0 {
                self.map.remove(key);
            }
        }
    }
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

pub struct FileObjectAppender<K, R, W, E = NoopErrorHandler> {
    writer: Arc<W>,
    error_handler: Arc<E>,
    map: Arc<DashMap<K, Arc<FileObjectBuffer<R>>>>,
    write_locks: Arc<RwLock<FileWriteLocks<K>>>,
    timeout: Duration,
    batch_size: usize,
}

impl<K, R, W, E> FileObjectAppender<K, R, W, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    W: FileAppendWriter<K, R>,
    E: ErrorHandler<K, R>,
{
    pub fn new(writer: Arc<W>, error_handler: Arc<E>, timeout: Duration, batch_size: usize) -> Self {
        Self {
            writer,
            error_handler,
            map: Arc::new(DashMap::new()),
            write_locks: Arc::new(RwLock::new(FileWriteLocks::new())),
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
                let write_locks = self.write_locks.clone();
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

                        let lock = write_locks.write().await.get_lock(&key_for_task).await;
                        let _ = lock.lock().await;

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
                            if let Err(e) = writer.write(&key_for_task, batch.clone()).await {
                                error_handler.on_write_error(&key_for_task, batch, &e);
                            }
                        }
                    }

                    // drop lock
                    write_locks.write().await.remove(&key_for_task);
                });
                // return buffer
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
        async fn write(&self, key: &String, records: Vec<String>) -> Result<()> {
            self.writes.lock().unwrap().push((key.clone(), records));
            Ok(())
        }
    }

    #[allow(dead_code)]
    struct MockErrorHandler {
        errors: Arc<Mutex<Vec<(String, Vec<String>, String)>>>,
    }
    impl ErrorHandler<String, String> for MockErrorHandler {
        fn on_write_error(&self, key: &String, batch: Vec<String>, error: &Error) {
            self.errors.lock().unwrap().push((key.clone(), batch, error.to_string()));
        }
    }

    #[tokio::test]
    async fn test_append_creates_sender_for_new_key() {
        let writer = Arc::new(MockWriter { writes: Arc::new(Mutex::new(Vec::new())) });
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        assert!(appender.map.contains_key("key1"));
    }

    #[tokio::test]
    async fn test_append_empty_vec_is_noop() {
        let writer = Arc::new(MockWriter { writes: Arc::new(Mutex::new(Vec::new())) });
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec![]).await;
        assert!(!appender.map.contains_key("key1"));
    }

    #[tokio::test]
    async fn test_append_reuses_existing_sender() {
        let writer = Arc::new(MockWriter { writes: Arc::new(Mutex::new(Vec::new())) });
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_secs(1), 10);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        let entry_count = appender.map.len();
        appender.append("key1".to_string(), vec!["b".to_string()]).await;
        assert_eq!(appender.map.len(), entry_count);
    }

    #[tokio::test]
    async fn test_flush_triggers_on_batch_size() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(MockWriter { writes: writes.clone() });
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_secs(60), 2);

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
        let writer = Arc::new(MockWriter { writes: writes.clone() });
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_millis(50), 100);

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
            async fn write(&self, _key: &String, _records: Vec<String>) -> Result<()> {
                Err(Error::IoError(
                    std::io::Error::new(std::io::ErrorKind::Other, "write failed").into(),
                ))
            }
        }

        let errors = Arc::new(Mutex::new(Vec::new()));
        let handler = Arc::new(MockErrorHandler { errors: errors.clone() });
        let appender: FileObjectAppender<String, String, FailingWriter, MockErrorHandler> =
            FileObjectAppender::new(Arc::new(FailingWriter {}), handler, Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let errors = errors.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "key1");
    }

    #[tokio::test]
    async fn test_new_channel_created_after_flush() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(MockWriter { writes: writes.clone() });
        let appender: FileObjectAppender<String, String, MockWriter> =
            FileObjectAppender::new(writer, Arc::new(NoopErrorHandler {}), Duration::from_millis(50), 100);

        appender.append("key1".to_string(), vec!["a".to_string()]).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(!appender.map.contains_key("key1"));

        appender.append("key1".to_string(), vec!["b".to_string()]).await;
        assert!(appender.map.contains_key("key1"));
    }
}
