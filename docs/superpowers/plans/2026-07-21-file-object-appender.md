# FileObjectAppender Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `FileObjectAppender<K, R, W, E>` for buffered batched writes to per-key files.

**Architecture:** Per-key `mpsc::UnboundedSender<Vec<R>>` in a `DashMap`. Background `flush_task` per key collects until batch_size or timeout, then removes map entry, closes sender, drains remaining, and calls `FileAppendWriter::write`. `Notify` enables external flush triggering.

**Tech Stack:** Rust, tokio, dashmap, async-trait

## Global Constraints

- Use `crate::error::Result<T>` (aliased to `std::result::Result<T, crate::error::Error>`) for all fallible operations
- Use `async_trait` for `FileAppendWriter` (existing project pattern)
- No new external dependencies beyond what's already in `Cargo.toml` (tokio, dashmap, async-trait, serde, etc.)
- Follow existing code style: no comments in implementation

---
### File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `kai-file/src/appender.rs` | Create | All types: traits, NoopErrorHandler, FileObjectAppender, flush_task, tests |
| `kai-file/src/lib.rs` | Modify | Add `pub mod appender;` and re-exports |

---

### Task 1: Define Traits + NoopErrorHandler

**Files:**
- Create: `kai-file/src/appender.rs` (top portion — traits + NoopErrorHandler + their tests)

**Interfaces:**
- Produces:
  - `pub trait FileAppendWriter<K, R>: Send + Sync + 'static` with `async fn write(&self, key: &K, records: Vec<R>) -> crate::error::Result<()>`
  - `pub trait ErrorHandler<K, R>: Send + Sync + 'static` with `fn on_write_error(&self, key: &K, batch: Vec<R>, error: &crate::error::Error)`
  - `pub struct NoopErrorHandler;` — empty impl of `ErrorHandler<K, R>`

- [ ] **Step 1: Update lib.rs to declare the module**

Edit `kai-file/src/lib.rs` — add `pub mod appender;` and re-export the public types.

```rust
pub mod appender;
pub mod error;
pub mod reverse_line_reader;
pub mod line_reader;
pub mod index;

pub use error::Error;
pub use reverse_line_reader::{ReverseLineReader, LineWithPosition};
pub use line_reader::LineReader;
pub use index::FileIndexContext;
pub use appender::{FileAppendWriter, ErrorHandler, NoopErrorHandler, FileObjectAppender};
```

- [ ] **Step 2: Create appender.rs with traits and NoopErrorHandler**

```rust
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
```

- [ ] **Step 3: Write test for traits — verify they can be implemented**

Add at the bottom of `appender.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    struct TestWriter;
    #[async_trait]
    impl FileAppendWriter<String, String> for TestWriter {
        async fn write(&self, _key: &String, _records: Vec<String>) -> crate::error::Result<()> {
            Ok(())
        }
    }

    struct TestErrorHandler;
    impl ErrorHandler<String, String> for TestErrorHandler {
        fn on_write_error(&self, _key: &String, _batch: Vec<String>, _error: &crate::error::Error) {}
    }

    #[tokio::test]
    async fn test_traits_are_implementable() {
        let writer = TestWriter;
        let handler = TestErrorHandler;
        let _appender: FileObjectAppender<String, String, TestWriter, TestErrorHandler> =
            FileObjectAppender::new(writer, handler, std::time::Duration::from_secs(10), 100);
    }
}
```

- [ ] **Step 4: Run test and verify it compiles**

```bash
cargo test -p kai-file test_traits_are_implementable -- --nocapture
```

Expected: PASS (will fail with `FileObjectAppender` not defined until Task 2 — that's expected)

- [ ] **Step 5: Commit**

```bash
git add kai-file/src/appender.rs kai-file/src/lib.rs
git commit -m "feat(kai-file): define FileAppendWriter and ErrorHandler traits"
```

---

### Task 2: FileObjectAppender Struct + append

**Files:**
- Modify: `kai-file/src/appender.rs`

**Interfaces:**
- Consumes: `FileAppendWriter`, `ErrorHandler`, `NoopErrorHandler` from Task 1
- Produces:
  - `pub struct FileObjectAppender<K, R, W, E = NoopErrorHandler>`
  - `impl<K, R, W, E> FileObjectAppender<K, R, W, E>` with:
    - `pub fn new(writer: W, error_handler: E, timeout: Duration, batch_size: usize) -> Self`
    - `fn get_or_create_sender(&self, key: &K) -> mpsc::UnboundedSender<Vec<R>>`
    - `pub async fn append(&self, key: K, records: Vec<R>)`

- [ ] **Step 1: Add struct definition, constructor, get_or_create_sender, append**

Between the traits and the tests, add:

```rust
use std::marker::PhantomData;
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
                    key_for_task, tx, rx, notify, map, writer, error_handler, timeout, batch_size,
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
```

Note: `flush_task` is declared but not yet defined — it will be implemented in Task 3. Add a minimal placeholder so the code compiles:

```rust
impl<K, R, W, E> FileObjectAppender<K, R, W, E>
where
    // ... same bounds
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
        // Placeholder — will be replaced in Task 3
        let _ = (key, tx, rx, notify, map, writer, error_handler, timeout, batch_size);
    }
}
```

- [ ] **Step 2: Write tests for append**

Replace the existing test with comprehensive tests:

```rust
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

        // First append should create a new channel entry
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
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
cargo test -p kai-file test_append_creates_sender_for_new_key test_append_empty_vec_is_noop test_append_reuses_existing_sender -- --nocapture
```

Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(kai-file): add FileObjectAppender struct with append method"
```

---

### Task 3: flush_task Implementation

**Files:**
- Modify: `kai-file/src/appender.rs` (replace the flush_task placeholder)

**Interfaces:**
- Consumes: All types from Task 1, `get_or_create_sender` from Task 2
- Produces: Complete `flush_task` implementation with collection loop, close/drain/write logic

- [ ] **Step 1: Replace flush_task placeholder with full implementation**

```rust
#[allow(clippy::too_many_arguments)]
async fn flush_task(
    key: K,
    tx: mpsc::UnboundedSender<Vec<R>>,
    mut rx: mpsc::UnboundedReceiver<Vec<R>>,
    notify: Arc<Notify>,
    map: DashMap<K, (mpsc::UnboundedSender<Vec<R>>, Arc<Notify>)>,
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
    tx.close();

    if !batch.is_empty() {
        while let Ok(chunk) = rx.try_recv() {
            batch.extend(chunk);
        }
        if let Err(e) = writer.write(&key, batch).await {
            error_handler.on_write_error(&key, batch, e);
        }
    }
}
```

- [ ] **Step 2: Write tests for flush_task**

Add these tests:

```rust
#[tokio::test]
async fn test_flush_triggers_on_batch_size() {
    let writes = Arc::new(Mutex::new(Vec::new()));
    let writer = MockWriter { writes: writes.clone() };
    let appender: FileObjectAppender<String, String, MockWriter> =
        FileObjectAppender::new(writer, NoopErrorHandler, Duration::from_secs(60), 2);

    appender.append("key1".to_string(), vec!["a".to_string()]).await;
    appender.append("key1".to_string(), vec!["b".to_string()]).await;

    // Wait for flush task to process
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
            Err(crate::error::Error::ExternalError("write failed".into()))
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

    // Map entry should be removed after flush
    assert!(!appender.map.contains_key("key1"));

    // New append creates a new channel
    appender.append("key1".to_string(), vec!["b".to_string()]).await;
    assert!(appender.map.contains_key("key1"));
}
```

- [ ] **Step 3: Run all tests**

```bash
cargo test -p kai-file -- --nocapture
```

Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(kai-file): implement flush_task with batch/timeout triggers and error handling"
```

---

### Task 4: flush_key / flush_all

**Files:**
- Modify: `kai-file/src/appender.rs` (add flush_key and flush_all methods)

**Interfaces:**
- Consumes: `Notify` from map entry
- Produces:
  - `pub async fn flush_key(&self, key: &K)`
  - `pub async fn flush_all(&self)`

- [ ] **Step 1: Add flush_key and flush_all methods**

Within the `impl` block, add:

```rust
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
```

- [ ] **Step 2: Write tests for flush_key and flush_all**

```rust
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
```

- [ ] **Step 3: Run all tests**

```bash
cargo test -p kai-file -- --nocapture
```

Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(kai-file): add flush_key and flush_all methods"
```
