# FileIndexContext 行号索引实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 FileIndexContext 增加每 1000 行的 page 索引，实现按行号区间和最后 N 行的查询功能。

**Architecture:** 新增 LineReader（正向读取器）替代原有的反向扫描构建索引；FilePosition 增加行号字段；FileIndexContext 增加 page_index / line_count / last_pos / last_line_index 四个 DashMap；新查询方法用 page 索引定位再正向读取；query_last_n 复用现有的 ReverseLineReader。

**Tech Stack:** Rust, tokio, serde_json

## Global Constraints

- 不删除注释
- 新增 LineReader 的测试种类需与 ReverseLineReader 一一对应
- 现有 FileIndexContext 测试（`test_mark_and_query_channel`）的语义行为不能变，只适配返回值类型
- 查询范围超过文件实际范围时不报错，返回交集
- 所有返回记录统一使用 `(u64, Arc<RR>)` 格式（行下标, 记录）

---

### Task 1: LineReader 正向读取器

**Files:**
- Create: `kai-file/src/line_reader.rs`
- Test: 内联 `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `kai-file/src/error.rs` 的 `Result<T>`，`kai-file/src/reverse_line_reader.rs` 的 `LineWithPosition`
- Produces: `pub struct LineReader`，`pub async fn next_line(&mut self) -> Result<Option<LineWithPosition>>`

- [ ] **Step 1: 创建 line_reader.rs 骨架，写入测试**

```rust
use std::collections::LinkedList;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use crate::error::Result;
use crate::reverse_line_reader::LineWithPosition;

const BUFFER_SIZE: usize = 4096;

pub struct LineReader {
    file: File,
    end_pos: u64,
    current_pos: u64,
    line_start_pos: u64,
    buffer: Vec<u8>,
    buffer_len: usize,
    leftover: Vec<u8>,
}

impl LineReader {
    pub async fn new(
        path: impl AsRef<std::path::Path>,
        start_pos: Option<u64>,
        end_pos: Option<u64>,
    ) -> Result<Self> {
        let mut file = File::open(path).await?;
        let file_size = file.seek(SeekFrom::End(0)).await?;
        let end_pos = end_pos.map(|e| e.min(file_size)).unwrap_or(file_size);
        let start_pos = start_pos.map(|s| s.min(end_pos)).unwrap_or(0);
        file.seek(SeekFrom::Start(start_pos)).await?;
        Ok(Self {
            file,
            end_pos,
            current_pos: start_pos,
            line_start_pos: start_pos,
            buffer: vec![0; BUFFER_SIZE],
            buffer_len: 0,
            leftover: Vec::new(),
        })
    }

    pub async fn next_line(&mut self) -> Result<Option<LineWithPosition>> {
        loop {
            if self.buffer_len == 0 {
                let remaining = self.end_pos - self.current_pos;
                if remaining == 0 {
                    if self.leftover.is_empty() {
                        return Ok(None);
                    }
                    let line_data = std::mem::take(&mut self.leftover);
                    let start_pos = self.line_start_pos;
                    let end_pos = self.current_pos;
                    let line = String::from_utf8(line_data)?;
                    self.line_start_pos = self.current_pos;
                    return Ok(Some(LineWithPosition { line, start_pos, end_pos }));
                }
                let read_size = std::cmp::min(BUFFER_SIZE, remaining as usize);
                self.file.read_exact(&mut self.buffer[..read_size]).await?;
                self.buffer_len = read_size;
            }

            for i in 0..self.buffer_len {
                if self.buffer[i] == b'\n' {
                    let line_start_pos = self.line_start_pos;
                    let line_end_pos = self.current_pos + i as u64 + 1;

                    let mut line_data = if self.leftover.is_empty() {
                        self.buffer[..i].to_vec()
                    } else {
                        let mut combined = std::mem::take(&mut self.leftover);
                        combined.extend_from_slice(&self.buffer[..i]);
                        combined
                    };

                    // 去掉结尾 \r（\r\n 的情况）
                    if line_data.last() == Some(&b'\r') {
                        line_data.pop();
                    }

                    let bytes_consumed = i + 1;
                    self.current_pos += bytes_consumed as u64;
                    self.buffer.copy_within(bytes_consumed.., 0);
                    self.buffer_len -= bytes_consumed;
                    self.line_start_pos = self.current_pos;

                    let line = String::from_utf8(line_data)?;
                    return Ok(Some(LineWithPosition {
                        line,
                        start_pos: line_start_pos,
                        end_pos: line_end_pos,
                    }));
                }
            }

            // 当前 buffer 中无换行符 → 整块移入 leftover，继续读
            self.leftover.extend_from_slice(&self.buffer[..self.buffer_len]);
            self.current_pos += self.buffer_len as u64;
            self.buffer_len = 0;
        }
    }

    pub fn current_pos(&self) -> u64 {
        self.current_pos
    }
}
```

- [ ] **Step 2: 验证编译**

Run: `cargo build 2>&1 | head -20`
Expected: 编译通过

- [ ] **Step 3: 写入所有测试，每个边界场景一个测试函数**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tempfile::tempdir;

    async fn write_file(dir: &tempfile::TempDir, name: &str, content: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut file = File::create(&path).await.unwrap();
        file.write_all(content).await.unwrap();
        path
    }

    #[tokio::test]
    async fn test_line_reader_basic() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "basic.txt", b"line 1\nline 2\r\nline 3\nline 4").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0, end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 7, end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(),
            start_pos: 15, end_pos: 22,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 4".to_string(),
            start_pos: 22, end_pos: 28,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_empty_file() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "empty.txt", b"").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_single_line() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "single.txt", b"single line").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "single line".to_string(),
            start_pos: 0, end_pos: 11,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_newline() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ends_newline.txt", b"line 1\nline 2\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0, end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 7, end_pos: 14,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_crlf() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ends_crlf.txt", b"line 1\r\nline 2\r\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(),
            start_pos: 0, end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(),
            start_pos: 8, end_pos: 16,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_double_newline() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ends_double_nl.txt", b"line 1\nline 2\n\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(), start_pos: 0, end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(), start_pos: 7, end_pos: 14,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 14, end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_ends_with_double_crlf() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ends_double_crlf.txt", b"line 1\r\nline 2\r\n\r\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(), start_pos: 0, end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(), start_pos: 8, end_pos: 16,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 16, end_pos: 18,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_empty_lines_in_middle() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "empty_mid.txt", b"line 1\n\nline 2\nline 3").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(), start_pos: 0, end_pos: 7,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 7, end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(), start_pos: 8, end_pos: 15,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(), start_pos: 15, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_newline() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "only_nl.txt", b"\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 0, end_pos: 1,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_two_newlines() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "only_two_nl.txt", b"\n\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 0, end_pos: 1,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 1, end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_crlf() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "only_crlf.txt", b"\r\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 0, end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_only_two_crlfs() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "only_two_crlfs.txt", b"\r\n\r\n").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 0, end_pos: 2,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 2, end_pos: 4,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_empty_lines_in_middle_crlf() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "empty_mid_crlf.txt", b"line 1\r\n\r\nline 2\r\nline 3").await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 1".to_string(), start_pos: 0, end_pos: 8,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "".to_string(), start_pos: 8, end_pos: 10,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 2".to_string(), start_pos: 10, end_pos: 18,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "line 3".to_string(), start_pos: 18, end_pos: 24,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_start() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "cs.txt", "第一行\n第二行\r\n第三行\n第四行".as_bytes()).await;
        let mut reader = LineReader::new(&path, Some(13), None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "二行".to_string(), start_pos: 13, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三行".to_string(), start_pos: 21, end_pos: 31,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第四行".to_string(), start_pos: 31, end_pos: 40,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_end() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "ce.txt", "第一行\n第二行\r\n第三行\n第四行".as_bytes()).await;
        let mut reader = LineReader::new(&path, None, Some(27)).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第一行".to_string(), start_pos: 0, end_pos: 10,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第二行".to_string(), start_pos: 10, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三".to_string(), start_pos: 21, end_pos: 27,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_custom_start_end() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "cse.txt", "第一行\n第二行\r\n第三行\n第四行".as_bytes()).await;
        let mut reader = LineReader::new(&path, Some(13), Some(27)).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "二行".to_string(), start_pos: 13, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三".to_string(), start_pos: 21, end_pos: 27,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_long_line() {
        let dir = tempdir().unwrap();
        let long = "A".repeat(BUFFER_SIZE * 2 + 100);
        let path = write_file(&dir, "long.txt", format!("short\n{}\nend", long).as_bytes()).await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        let first = reader.next_line().await.unwrap().unwrap();
        assert_eq!(first.line, "short");
        let second = reader.next_line().await.unwrap().unwrap();
        assert_eq!(second.line.len(), BUFFER_SIZE * 2 + 100);
        assert_eq!(second.line, long);
        let third = reader.next_line().await.unwrap().unwrap();
        assert_eq!(third.line, "end");
        assert_eq!(reader.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_chinese() {
        let dir = tempdir().unwrap();
        let path = write_file(&dir, "chinese.txt", "第一行\n第二行\r\n第三行\n第四行".as_bytes()).await;
        let mut reader = LineReader::new(&path, None, None).await.unwrap();
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第一行".to_string(), start_pos: 0, end_pos: 10,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第二行".to_string(), start_pos: 10, end_pos: 21,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第三行".to_string(), start_pos: 21, end_pos: 31,
        }));
        assert_eq!(reader.next_line().await.unwrap(), Some(LineWithPosition {
            line: "第四行".to_string(), start_pos: 31, end_pos: 40,
        }));
        assert_eq!(reader.next_line().await.unwrap(), None);
    }
}
```

- [ ] **Step 4: 运行所有 LineReader 测试**

Run: `cargo test -p kai-file line_reader 2>&1`
Expected: 全部 PASS（14 个测试）

- [ ] **Step 5: Commit**

```bash
git add kai-file/src/line_reader.rs
git commit -m "feat(kai-file): add LineReader forward reader with full test coverage"
```

---

### Task 2: FilePosition 扩展 + FileIndexContext 新字段 + forward update

**Files:**
- Modify: `kai-file/src/index.rs`
- Modify: `kai-file/src/lib.rs`（导出 LineReader）

**Interfaces:**
- Consumes: Task 1 的 `LineReader`
- Produces: 更新后的 `FilePosition`，`FileIndexContext` 新字段和方法

- [ ] **Step 1: 在 lib.rs 中导出 LineReader**

```rust
// 在 kai-file/src/lib.rs 中新增
pub mod line_reader;
pub use line_reader::LineReader;
```

- [ ] **Step 2: 扩展 FilePosition**

```rust
#[derive(Debug, Clone)]
struct FilePosition {
    pub start_pos: u64,
    pub end_pos: u64,
    pub start_line_index: u64,
    pub end_line_index: u64,
}
```

- [ ] **Step 3: FileIndexContext 新增字段**

```rust
pub struct FileIndexContext<Q,K,R,RR,P> {
    _marker: PhantomData<(Q,R,RR)>,
    position_map_map: DashMap<K, FileIndexLock>,
    obsolete_set: DashSet<K>,
    all_obsolete_set: DashSet<K>,
    parser: P,
    // 新增
    page_index_map: DashMap<K, Arc<RwLock<BTreeMap<u64, u64>>>>,
    line_count_map: DashMap<K, Arc<RwLock<u64>>>,
    last_pos_map: DashMap<K, Arc<RwLock<u64>>>,
    last_line_index_map: DashMap<K, Arc<RwLock<u64>>>,
}
```

- [ ] **Step 4: 修改 `new` 初始化新字段**

```rust
pub fn new(parser: P) -> Self {
    Self {
        _marker: PhantomData,
        position_map_map: DashMap::new(),
        obsolete_set: DashSet::new(),
        all_obsolete_set: DashSet::new(),
        parser,
        page_index_map: DashMap::new(),
        line_count_map: DashMap::new(),
        last_pos_map: DashMap::new(),
        last_line_index_map: DashMap::new(),
    }
}
```

- [ ] **Step 5: 修改 `get_lock` 初始化新字段**

```rust
fn get_lock(&self, key: &K) -> FileIndexLock {
    let lock = self.position_map_map.entry(key.clone()).or_insert_with(|| {
        self.all_obsolete_set.insert(key.clone());
        self.obsolete_set.insert(key.clone());
        Arc::new(RwLock::new(BTreeMap::new()))
    }).clone();
    // 初始化新索引
    self.page_index_map.entry(key.clone()).or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())));
    self.line_count_map.entry(key.clone()).or_insert_with(|| Arc::new(RwLock::new(0)));
    self.last_pos_map.entry(key.clone()).or_insert_with(|| Arc::new(RwLock::new(0)));
    self.last_line_index_map.entry(key.clone()).or_insert_with(|| Arc::new(RwLock::new(0)));
    lock
}
```

- [ ] **Step 6: 实现 `update_forward` 核心方法**

```rust
async fn update_forward(
    mut guard: RwLockWriteGuard<'_, BTreeMap<String, FilePosition>>,
    mut page_guard: RwLockWriteGuard<'_, BTreeMap<u64, u64>>,
    mut line_count_guard: RwLockWriteGuard<'_, u64>,
    mut last_pos_guard: RwLockWriteGuard<'_, u64>,
    mut last_line_index_guard: RwLockWriteGuard<'_, u64>,
    file_path: impl AsRef<Path>,
    incremental: bool,
) -> Result<()> {
    if !incremental {
        guard.clear();
        page_guard.clear();
        *line_count_guard = 0;
        *last_pos_guard = 0;
        *last_line_index_guard = 0;
    }

    let start_pos = *last_pos_guard;
    let mut line_index = *last_line_index_guard;

    let mut reader = LineReader::new(file_path, Some(start_pos), None).await?;
    while let Some(line_with_pos) = reader.next_line().await? {
        let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;

        // 插入或合并到 time 索引
        match guard.entry(record.time().to_string()) {
            Entry::Occupied(mut entry) => {
                let min_start = entry.get().start_pos.min(line_with_pos.start_pos);
                let max_end = entry.get().end_pos.max(line_with_pos.end_pos);
                let min_line = entry.get().start_line_index.min(line_index);
                let max_line = entry.get().end_line_index.max(line_index + 1);
                entry.get_mut().start_pos = min_start;
                entry.get_mut().end_pos = max_end;
                entry.get_mut().start_line_index = min_line;
                entry.get_mut().end_line_index = max_line;
            }
            Entry::Vacant(entry) => {
                entry.insert(FilePosition {
                    start_pos: line_with_pos.start_pos,
                    end_pos: line_with_pos.end_pos,
                    start_line_index: line_index,
                    end_line_index: line_index + 1,
                });
            }
        }

        // page 索引
        if line_index % 1000 == 0 {
            page_guard.insert(line_index / 1000, line_with_pos.start_pos);
        }

        line_index += 1;
    }

    *line_count_guard = line_index;
    *last_pos_guard = reader.current_pos();
}
```

- [ ] **Step 7: 重构 `update_index` 和 `update_all_index`**

```rust
async fn update_index(&self, key: &K) -> Result<()> {
    let position_map = self.get_lock(key);
    let page_map = self.page_index_map.get(key).unwrap().clone();
    let line_count = self.line_count_map.get(key).unwrap().clone();
    let last_pos = self.last_pos_map.get(key).unwrap().clone();
    let last_line = self.last_line_index_map.get(key).unwrap().clone();

    let guard = position_map.write().await;
    let page_guard = page_map.write().await;
    let line_count_guard = line_count.write().await;
    let last_pos_guard = last_pos.write().await;
    let last_line_guard = last_line.write().await;

    let file_path = self.parser.get_path(key).await?;
    Self::update_forward(guard, page_guard, line_count_guard, last_pos_guard, last_line_guard, file_path, true).await
}

async fn update_all_index(&self, key: &K) -> Result<()> {
    let position_map = self.get_lock(key);
    let page_map = self.page_index_map.get(key).unwrap().clone();
    let line_count = self.line_count_map.get(key).unwrap().clone();
    let last_pos = self.last_pos_map.get(key).unwrap().clone();
    let last_line = self.last_line_index_map.get(key).unwrap().clone();

    let guard = position_map.write().await;
    let page_guard = page_map.write().await;
    let line_count_guard = line_count.write().await;
    let last_pos_guard = last_pos.write().await;
    let last_line_guard = last_line.write().await;

    let file_path = self.parser.get_path(key).await?;
    Self::update_forward(guard, page_guard, line_count_guard, last_pos_guard, last_line_guard, file_path, false).await
}
```

- [ ] **Step 8: 修改 `mark_all_obsolete`，重置 last_pos/last_line_index**

```rust
pub fn mark_all_obsolete(&self, key: &K) {
    self.all_obsolete_set.insert(key.clone());
    self.obsolete_set.insert(key.clone());
    // 重置游标，下次全量重建
    if let Some(entry) = self.last_pos_map.get(key) {
        *entry.value().write().unwrap() = 0;
    }
    if let Some(entry) = self.last_line_index_map.get(key) {
        *entry.value().write().unwrap() = 0;
    }
}
```

注意：`mark_all_obsolete` 在 `get_lock` 的 `or_insert_with` 中也会被调用，新 key 的初始状态已经是 last_pos=0/last_line_index=0，不需要额外处理。

- [ ] **Step 9: 编译验证**

Run: `cargo build -p kai-file 2>&1`
Expected: 编译通过

- [ ] **Step 10: Commit**

```bash
git add kai-file/src/lib.rs kai-file/src/index.rs
git commit -m "refactor(kai-file): add page index fields and forward-based update"
```

---

### Task 3: query_by_line_range + query_last_n + query_reverse 返回值适配

**Files:**
- Modify: `kai-file/src/index.rs`

**Interfaces:**
- Produces: `query_by_line_range(&self, key: &K, start_line: u64, end_line: u64) -> Result<Vec<(u64, Arc<RR>)>>`
- Produces: `query_last_n(&self, key: &K, n: u64) -> Result<Vec<(u64, Arc<RR>)>>`
- Modifies: `query_reverse` 返回 `LinkedList<(u64, Arc<RR>)>`
- Modifies: `query_all` 返回 `Result<Vec<(u64, Arc<RR>)>>`

- [ ] **Step 1: 修改 `query_reverse` 返回带行号的元组**

```rust
async fn query_reverse(&self, key: &K, start: &str, end: &str) -> Result<LinkedList<(u64, Arc<RR>)>> {
    let position_map = self.get_lock(key);

    if self.obsolete_set.remove(key).is_some() {
        if self.all_obsolete_set.contains(key) {
            if let Err(e) = self.update_all_index(key).await {
                self.all_obsolete_set.insert(key.clone());
                self.obsolete_set.insert(key.clone());
                return Err(e);
            }
        } else {
            if let Err(e) = self.update_index(key).await {
                self.obsolete_set.insert(key.clone());
                return Err(e);
            }
        }
    }

    let mut results = LinkedList::new();
    let guard = position_map.read().await;
    let mut start_pos: Option<u64> = None;
    let mut end_pos: Option<u64> = None;
    if let Some((_, position)) = guard.range(start.to_string()..=end.to_string()).next() {
        start_pos = Some(position.start_pos);
    }
    if let Some((_, position)) = guard.range(start.to_string()..=end.to_string()).next_back() {
        end_pos = Some(position.end_pos);
    }
    if start_pos.is_some() && end_pos.is_some() {
        let file_path = self.parser.get_path(key).await?;
        let mut reader = ReverseLineReader::new(file_path, start_pos, end_pos).await?;
        while let Some(line_with_pos) = reader.next_line().await? {
            let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
            let record = self.parser.combine_record(key, &record);

            // 从 BTreeMap 查找行号
            let line_index = guard.range(start.to_string()..=end.to_string())
                .find(|(_, pos)| pos.start_pos == line_with_pos.start_pos)
                .map(|(_, pos)| pos.start_line_index)
                .unwrap_or(0);

            results.push_front((line_index, Arc::new(record)));
        }
    }
    Ok(results)
}
```

- [ ] **Step 2: 修改 `query_all`**

```rust
pub async fn query_all(&self, query: Q) -> Result<Vec<(u64, Arc<RR>)>> {
    let key_with_range = self.parser.parse_query(query);
    let mut results_list = LinkedList::new();
    for (key, (start, end)) in key_with_range {
        let results = self.query_reverse(&key, start.as_str(), end.as_str()).await?;
        results_list.push_back(results);
    }
    let mut len = 0;
    for results in results_list.iter() {
        len += results.len();
    }
    let mut results = Vec::with_capacity(len);
    while let Some(mut records) = results_list.pop_front() {
        while let Some(record) = records.pop_front() {
            results.push(record);
        }
    }
    Ok(results)
}
```

- [ ] **Step 3: 实现 `query_by_line_range`**

```rust
pub async fn query_by_line_range(&self, key: &K, start_line: u64, end_line: u64) -> Result<Vec<(u64, Arc<RR>)>> {
    let position_map = self.get_lock(key);

    if self.obsolete_set.remove(key).is_some() {
        if self.all_obsolete_set.contains(key) {
            if let Err(e) = self.update_all_index(key).await {
                self.all_obsolete_set.insert(key.clone());
                self.obsolete_set.insert(key.clone());
                return Err(e);
            }
        } else {
            if let Err(e) = self.update_index(key).await {
                self.obsolete_set.insert(key.clone());
                return Err(e);
            }
        }
    }

    // 获取总行数
    let line_count = {
        let guard = self.line_count_map.get(key).unwrap();
        *guard.read().await
    };

    // 钳制范围
    let actual_start = start_line.min(line_count);
    let actual_end = end_line.min(line_count);
    if actual_start >= actual_end {
        return Ok(Vec::new());
    }

    // 找 page 起点
    let page = actual_start / 1000;
    let start_byte = {
        let page_guard = self.page_index_map.get(key).unwrap();
        let pages = page_guard.read().await;
        pages.get(&page).copied().unwrap_or(0)
    };

    // 正向读取
    let file_path = self.parser.get_path(key).await?;
    let mut reader = LineReader::new(file_path, Some(start_byte), None).await?;
    let mut results = Vec::new();
    let mut line_idx = page * 1000;

    while let Some(line_with_pos) = reader.next_line().await? {
        if line_idx >= actual_end {
            break;
        }
        if line_idx >= actual_start {
            let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
            let record = self.parser.combine_record(key, &record);
            results.push((line_idx, Arc::new(record)));
        }
        line_idx += 1;
    }

    Ok(results)
}
```

- [ ] **Step 4: 实现 `query_last_n`**

```rust
pub async fn query_last_n(&self, key: &K, n: u64) -> Result<Vec<(u64, Arc<RR>)>> {
    let position_map = self.get_lock(key);

    if self.obsolete_set.remove(key).is_some() {
        if self.all_obsolete_set.contains(key) {
            if let Err(e) = self.update_all_index(key).await {
                self.all_obsolete_set.insert(key.clone());
                self.obsolete_set.insert(key.clone());
                return Err(e);
            }
        } else {
            if let Err(e) = self.update_index(key).await {
                self.obsolete_set.insert(key.clone());
                return Err(e);
            }
        }
    }

    let line_count = {
        let guard = self.line_count_map.get(key).unwrap();
        *guard.read().await
    };

    if n == 0 || line_count == 0 {
        return Ok(Vec::new());
    }

    if n >= line_count {
        return self.query_by_line_range(key, 0, line_count).await;
    }

    // 用 ReverseLineReader 反向读最后 n 行
    let file_path = self.parser.get_path(key).await?;
    let mut reader = ReverseLineReader::new(file_path, None, None).await?;
    let mut results = LinkedList::new();

    for _ in 0..n {
        if let Some(line_with_pos) = reader.next_line().await? {
            let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
            let record = self.parser.combine_record(key, &record);
            let line_index = line_count - results.len() as u64 - 1;
            results.push_front((line_index, Arc::new(record)));
        } else {
            break;
        }
    }

    Ok(results.into_iter().collect())
}
```

- [ ] **Step 5: 编译验证**

Run: `cargo build -p kai-file 2>&1`
Expected: 编译通过

- [ ] **Step 6: Commit**

```bash
git add kai-file/src/index.rs kai-file/src/line_reader.rs
git commit -m "feat(kai-file): add query_by_line_range and query_last_n methods"
```

---

### Task 4: 适配现有测试 + 新增完整测试

**Files:**
- Modify: `kai-file/src/index.rs`（现有测试适配）

- [ ] **Step 1: 适配 `test_mark_and_query_channel`**

改为解包 `(line_index, Arc<record>)`，验证行号：

```rust
#[tokio::test]
async fn test_mark_and_query_channel() {
    // ... 原有 setup 不变 ...
    let parser = TestRecordParser { root_dir: dir };
    let indexer = FileIndexContext::new(parser);

    // query range excludes A (line 0), includes B (line 1)
    let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1);          // line index 1
    assert_eq!(results[0].1.content.as_str(), "B");

    // mark + query — incremental load picks up C (line 2)
    indexer.mark_obsolete(&date.to_string());
    let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, 1);
    assert_eq!(results[0].1.content.as_str(), "B");
    assert_eq!(results[1].0, 2);
    assert_eq!(results[1].1.content.as_str(), "C");

    // mark_all — full rebuild
    remove_file(&file_path).await.unwrap();
    append_jsonl(date, r#"{"content":"D","time":"2026-06-24 08:30:00"}"#).await;
    append_jsonl(date, r#"{"content":"E","time":"2026-06-24 10:30:00"}"#).await;
    append_jsonl(date, r#"{"content":"F","time":"2026-06-24 14:00:00"}"#).await;

    indexer.mark_all_obsolete(&date.to_string());
    let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1);  // E is the 2nd line in the new file (index 1)
    assert_eq!(results[0].1.content.as_str(), "E");
}
```

- [ ] **Step 2: 运行现有测试确认通过**

Run: `cargo test -p kai-file test_mark_and_query_channel -- --nocapture 2>&1`
Expected: PASS

- [ ] **Step 3: 新增 `test_query_by_line_range`**

```rust
#[tokio::test]
async fn test_query_by_line_range() {
    let dir = init_test_dir();
    let date = "2026-07-19";
    let file_path = get_path(&dir, date).await.unwrap();
    if file_path.exists() {
        remove_file(&file_path).await.unwrap();
    }

    for i in 0..10 {
        let h = 8 + i;
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
    }

    let parser = TestRecordParser { root_dir: dir };
    let indexer = FileIndexContext::new(parser);

    let results = indexer.query_by_line_range(&date.to_string(), 2, 5).await.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, 2);
    assert_eq!(results[0].1.content.as_str(), "L2");
    assert_eq!(results[1].0, 3);
    assert_eq!(results[1].1.content.as_str(), "L3");
    assert_eq!(results[2].0, 4);
    assert_eq!(results[2].1.content.as_str(), "L4");
}
```

- [ ] **Step 4: 新增 `test_query_by_line_range_empty`**

```rust
#[tokio::test]
async fn test_query_by_line_range_empty() {
    let dir = init_test_dir();
    let date = "2026-07-19";
    let file_path = get_path(&dir, date).await.unwrap();
    if file_path.exists() {
        remove_file(&file_path).await.unwrap();
    }

    for i in 0..5 {
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, 8+i)).await;
    }

    let parser = TestRecordParser { root_dir: dir };
    let indexer = FileIndexContext::new(parser);

    // 空区间
    let results = indexer.query_by_line_range(&date.to_string(), 3, 3).await.unwrap();
    assert_eq!(results.len(), 0);

    // start > end
    let results = indexer.query_by_line_range(&date.to_string(), 5, 3).await.unwrap();
    assert_eq!(results.len(), 0);
}
```

- [ ] **Step 5: 新增 `test_query_by_line_range_oob`**

```rust
#[tokio::test]
async fn test_query_by_line_range_oob() {
    let dir = init_test_dir();
    let date = "2026-07-19";
    let file_path = get_path(&dir, date).await.unwrap();
    if file_path.exists() {
        remove_file(&file_path).await.unwrap();
    }

    for i in 0..10 {
        let h = 8 + i;
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
    }

    let parser = TestRecordParser { root_dir: dir };
    let indexer = FileIndexContext::new(parser);

    // end 超出范围 → 截断到实际行数
    let results = indexer.query_by_line_range(&date.to_string(), 5, 20).await.unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, 5);
    assert_eq!(results[4].0, 9);

    // 全部超出
    let results = indexer.query_by_line_range(&date.to_string(), 10, 20).await.unwrap();
    assert_eq!(results.len(), 0);
}
```

- [ ] **Step 6: 新增 `test_query_last_n`**

```rust
#[tokio::test]
async fn test_query_last_n() {
    let dir = init_test_dir();
    let date = "2026-07-19";
    let file_path = get_path(&dir, date).await.unwrap();
    if file_path.exists() {
        remove_file(&file_path).await.unwrap();
    }

    for i in 0..10 {
        let h = 8 + i;
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
    }

    let parser = TestRecordParser { root_dir: dir };
    let indexer = FileIndexContext::new(parser);

    let results = indexer.query_last_n(&date.to_string(), 3).await.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, 7);
    assert_eq!(results[0].1.content.as_str(), "L7");
    assert_eq!(results[1].0, 8);
    assert_eq!(results[1].1.content.as_str(), "L8");
    assert_eq!(results[2].0, 9);
    assert_eq!(results[2].1.content.as_str(), "L9");
}
```

- [ ] **Step 7: 新增 `test_query_last_n_overflow`**

```rust
#[tokio::test]
async fn test_query_last_n_overflow() {
    let dir = init_test_dir();
    let date = "2026-07-19";
    let file_path = get_path(&dir, date).await.unwrap();
    if file_path.exists() {
        remove_file(&file_path).await.unwrap();
    }

    for i in 0..5 {
        let h = 8 + i;
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
    }

    let parser = TestRecordParser { root_dir: dir };
    let indexer = FileIndexContext::new(parser);

    // n > 总行数 → 返回全部
    let results = indexer.query_last_n(&date.to_string(), 100).await.unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, 0);
    assert_eq!(results[4].0, 4);

    // n = 0
    let results = indexer.query_last_n(&date.to_string(), 0).await.unwrap();
    assert_eq!(results.len(), 0);
}
```

- [ ] **Step 8: 新增 `test_incremental_update_line_index`**

```rust
#[tokio::test]
async fn test_incremental_update_line_index() {
    let dir = init_test_dir();
    let date = "2026-07-19";
    let file_path = get_path(&dir, date).await.unwrap();
    if file_path.exists() {
        remove_file(&file_path).await.unwrap();
    }

    // 写入1005行
    for i in 0..1005 {
        let h = i / 60;
        let m = i % 60;
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
    }

    let parser = TestRecordParser { root_dir: dir };
    let indexer = FileIndexContext::new(parser);

    // 触发全量更新
    let results = indexer.query_by_line_range(&date.to_string(), 0, 1005).await.unwrap();
    assert_eq!(results.len(), 1005);

    // 验证 page 索引: key 0 (line 0), key 1 (line 1000)
    let page_guard = indexer.page_index_map.get(&date.to_string()).unwrap();
    let pages = page_guard.read().await;
    assert_eq!(pages.len(), 2);
    assert!(pages.contains_key(&0));
    assert!(pages.contains_key(&1));

    // 追加10行（总1015）
    for i in 1005..1015 {
        let h = i / 60;
        let m = i % 60;
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
    }

    // 增量更新
    indexer.mark_obsolete(&date.to_string());
    let results = indexer.query_last_n(&date.to_string(), 10).await.unwrap();
    assert_eq!(results.len(), 10);
    assert_eq!(results[0].0, 1005);
    assert_eq!(results[9].0, 1014);

    // page 索引不变（未跨越新page边界，1014 < 2000）
    let page_guard = indexer.page_index_map.get(&date.to_string()).unwrap();
    let pages = page_guard.read().await;
    assert_eq!(pages.len(), 2);

    // 继续追加到跨越2000边界（共2005行）
    for i in 1015..2005 {
        let h = i / 60;
        let m = i % 60;
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
    }

    indexer.mark_obsolete(&date.to_string());
    let results = indexer.query_by_line_range(&date.to_string(), 2000, 2005).await.unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, 2000);

    // page 索引新增 key 2
    let page_guard = indexer.page_index_map.get(&date.to_string()).unwrap();
    let pages = page_guard.read().await;
    assert_eq!(pages.len(), 3);
    assert!(pages.contains_key(&2));
}
```

- [ ] **Step 9: 新增 `test_page_index`**

```rust
#[tokio::test]
async fn test_page_index() {
    let dir = init_test_dir();
    let date = "2026-07-19";
    let file_path = get_path(&dir, date).await.unwrap();
    if file_path.exists() {
        remove_file(&file_path).await.unwrap();
    }

    // 3005行
    for i in 0..3005 {
        let h = i / 60;
        let m = i % 60;
        append_jsonl(date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
    }

    let parser = TestRecordParser { root_dir: dir };
    let indexer = FileIndexContext::new(parser);

    // 触发全量更新
    let results = indexer.query_by_line_range(&date.to_string(), 0, 3005).await.unwrap();
    assert_eq!(results.len(), 3005);

    // page 索引4个 key: 0,1,2,3
    let page_guard = indexer.page_index_map.get(&date.to_string()).unwrap();
    let pages = page_guard.read().await;
    assert_eq!(pages.len(), 4);
    assert!(pages.contains_key(&0));
    assert!(pages.contains_key(&1));
    assert!(pages.contains_key(&2));
    assert!(pages.contains_key(&3));

    // 跨页范围查询
    let results = indexer.query_by_line_range(&date.to_string(), 1500, 2500).await.unwrap();
    assert_eq!(results.len(), 1000);
    assert_eq!(results[0].0, 1500);
    assert_eq!(results[999].0, 2499);

    // 最后几行
    let results = indexer.query_last_n(&date.to_string(), 5).await.unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, 3000);
    assert_eq!(results[4].0, 3004);
}
```

- [ ] **Step 10: 运行全部测试**

Run: `cargo test -p kai-file 2>&1`
Expected: 全部 PASS

- [ ] **Step 11: Commit**

```bash
git add kai-file/src/index.rs
git commit -m "test(kai-file): adapt existing tests and add line index tests"
```
