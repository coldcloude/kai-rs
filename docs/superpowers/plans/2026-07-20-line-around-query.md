# 行号前后查询 API 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 `query_by_line_range` / `query_last_n` 替换为 `query_after` / `query_before` / `query_first` / `query_last`。

**Architecture:** 单文件改动 `kai-file/src/index.rs`：删除原方法，新增四个方法；`query_before` 用 `ReverseLineReader` 反向读取；`query_after` / `query_first` 用 `LineReader` 正向读取；`query_last` 退化为 `query_before`。

**Tech Stack:** Rust, tokio

## Global Constraints

- `query_after` 正向读取，`query_before` 反向读取
- `line` 越界时返回 `Err`（`n == 0` 或 `end_line_index == 0` 时不触发错误）
- 数量不足时返回已取到的行，不报错
- `query_first(n)` = `query_after(0, n)`
- `query_last(n)` = `query_before(end_line_index - 1, n)`
- `query_before` 结果按正序返回
- 删除 `query_by_line_range`、`query_last_n`
- 现有通过 `query_by_line_range` 和 `query_last_n` 的测试改为新API

---

### Task 1: 替换 query_by_line_range / query_last_n

**Files:**
- Modify: `kai-file/src/index.rs`

**Interfaces:**
- Remove: `query_by_line_range(start_line, end_line) -> Vec<(u32, R)>`
- Remove: `query_last_n(n) -> Vec<(u32, R)>`
- Add: `query_after(line, n) -> Vec<(u32, R)>`
- Add: `query_before(line, n) -> Vec<(u32, R)>`
- Add: `query_first(n) -> Vec<(u32, R)>`
- Add: `query_last(n) -> Vec<(u32, R)>`

#### FileIndices 层

- [ ] **Step 1: 删除 `query_by_line_range` 和 `query_last_n`**

删除 `FileIndices` 上这两个方法。

- [ ] **Step 2: 实现 `query_after`**

```rust
pub async fn query_after<R: Record>(
    &self, file_path: impl AsRef<Path>, line: u32, n: u32,
) -> Result<Vec<(u32, R)>> {
    if n == 0 || line >= self.end_line_index {
        return if line >= self.end_line_index && self.end_line_index > 0 {
            Err(crate::error::Error::ExternalError(
                format!("line {} out of range, end_line_index={}", line, self.end_line_index).into()
            ))
        } else {
            Ok(Vec::new())
        };
    }

    let page = (line / 1000) as usize;
    let start_byte = self.page_index.get(page).copied().unwrap_or(0);
    let mut reader = LineReader::new(file_path, Some(start_byte), None).await?;
    let mut results = Vec::new();
    let mut line_idx = page as u32 * 1000;

    while let Some(line_with_pos) = reader.next_line().await? {
        if line_idx >= line + n {
            break;
        }
        if line_idx >= line {
            let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
            results.push((line_idx, record));
        }
        line_idx += 1;
    }

    Ok(results)
}
```

- [ ] **Step 3: 实现 `query_before`**

```rust
pub async fn query_before<R: Record>(
    &self, file_path: impl AsRef<Path>, line: u32, n: u32,
) -> Result<Vec<(u32, R)>> {
    if n == 0 || self.end_line_index == 0 {
        return Ok(Vec::new());
    }
    if line >= self.end_line_index {
        return Err(crate::error::Error::ExternalError(
            format!("line {} out of range, end_line_index={}", line, self.end_line_index).into()
        ));
    }

    let page = (line / 1000) as usize;
    let end_page = page + 1;
    let end_byte = self.page_index.get(end_page).copied().unwrap_or(self.end_pos);
    let start_byte = self.page_index.get(page).copied().unwrap_or(0);

    let mut page_end = end_byte;
    let mut results = LinkedList::new();
    let mut page_idx = page;

    loop {
        let page_start = self.page_index.get(page_idx).copied().unwrap_or(0);
        let mut reader = ReverseLineReader::new(&file_path, Some(page_start), Some(page_end)).await?;

        while let Some(line_with_pos) = reader.next_line().await? {
            // 计算当前行的行号（反向读取时，行号递减）
            // 该行在文件中的 start_pos 对应 page_start + ... 不太好算
            // 改为用 page_start 为基准，从 page 的开始扫描到结束
        }
        break; // temp
    }
    // 简化实现...
    Ok(results.into_iter().collect())
}
```

等一下，`ReverseLineReader` 读到的行是反向的，需要行号。但 `ReverseLineReader` 返回的 `LineWithPosition` 包含 `start_pos` / `end_pos`。我们可以通过 `time_index` 或 `page_index` 来确定行号。

更简单的实现：使用 `LineReader` 从 page 的起始位置正向扫描到包含 line 行的位置，收集前 n 行。

**但用户要求反向读取**。那只能用 `ReverseLineReader` 并在读完 page 后通过 `start_pos` 查 `time_index` 或 page 边界推算行号。

简化方案：用 `ReverseLineReader` 读取一个 page，读到的行用 `start_pos` 与 `page_index` 结合，用 `page_start + (line_idx - page*1000) * avg_line_len` 近似... 不，这太粗糙了。

**实际方案**：使用 `ReverseLineReader` 从 page 末尾反向读整个 page，收集所有行，然后根据 `start_pos` 和 `time_index` 或 `page_index` 确定行号，选出行号 ≤ line 的最近 n 行。

```rust
pub async fn query_before<R: Record>(
    &self, file_path: impl AsRef<Path>, line: u32, n: u32,
) -> Result<Vec<(u32, R)>> {
    if n == 0 || self.end_line_index == 0 {
        return Ok(Vec::new());
    }
    if line >= self.end_line_index {
        return Err(...);
    }

    let page = (line / 1000) as usize;
    let page_end_byte = self.page_index.get(page + 1).copied().unwrap_or(self.end_pos);
    let page_start_byte = self.page_index.get(page).copied().unwrap_or(0);

    // 反向读取整个 page → 收集所有行
    let mut reader = ReverseLineReader::new(&file_path, Some(page_start_byte), Some(page_end_byte)).await?;
    let mut all_lines: LinkedList<(u32, R)> = LinkedList::new();
    let mut line_cursor = page as u32 * 1000;

    // 正向扫一遍得到行号... 不对，还是得正向读。
    // 
    // 更好的办法：先用 LineReader 正向读该 page，收集所有行（含行号）
    // 然后取 ≤ line 的最后 n 行。
    //
    // 但用户要求反向读取。
    //
    // 折中：用 ReverseLineReader 读 page，读到的先 push_front，
    // 这样在结果列表中就是正序的。需要行号？没法从 ReverseLineReader 直接得到行号。
    // 
    // 用 time_index 查：line_with_pos.start_pos → time_index → start_line_index
    // time_index 的 FilePosition 有 start_line_index 字段。
```

实际可行方案：利用 `time_index` 通过 `start_pos` 查行号。

```rust
pub async fn query_before<R: Record>(
    &self, file_path: impl AsRef<Path>, line: u32, n: u32,
) -> Result<Vec<(u32, R)>> {
    if n == 0 || self.end_line_index == 0 {
        return Ok(Vec::new());
    }
    if line >= self.end_line_index {
        return Err(crate::error::Error::ExternalError(
            format!("line {} out of range, end_line_index={}", line, self.end_line_index).into()
        ));
    }

    // 预构建 start_pos → start_line_index 映射（O(m)，m=分钟级条目数）
    let pos_to_line: HashMap<u64, u32> = self.time_index.iter()
        .map(|(_, fp)| (fp.start_pos, fp.start_line_index))
        .collect();

    let page = (line / 1000) as usize;
    let mut remaining = n;
    let mut results = LinkedList::new();
    let mut page_idx = page;

    loop {
        let end_byte = self.page_index.get(page_idx + 1).copied().unwrap_or(self.end_pos);
        let start_byte = self.page_index.get(page_idx).copied().unwrap_or(0);

        let mut reader = ReverseLineReader::new(&file_path, Some(start_byte), Some(end_byte)).await?;
        while let Some(line_with_pos) = reader.next_line().await? {
            let line_idx = pos_to_line.get(&line_with_pos.start_pos).copied().unwrap_or(u32::MAX);
            if line_idx <= line {
                let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
                results.push_front((line_idx, record));
                remaining -= 1;
                if remaining == 0 {
                    return Ok(results.into_iter().collect());
                }
            }
        }

        if page_idx == 0 {
            break;
        }
        page_idx -= 1;
    }

    Ok(results.into_iter().collect())
}
```

- [ ] **Step 4: 实现 `query_first` 和 `query_last`**

```rust
pub async fn query_first<R: Record>(
    &self, file_path: impl AsRef<Path>, n: u32,
) -> Result<Vec<(u32, R)>> {
    self.query_after::<R>(file_path, 0, n).await
}

pub async fn query_last<R: Record>(
    &self, file_path: impl AsRef<Path>, n: u32,
) -> Result<Vec<(u32, R)>> {
    if n == 0 || self.end_line_index == 0 {
        return Ok(Vec::new());
    }
    self.query_before::<R>(file_path, self.end_line_index - 1, n).await
}
```

- [ ] **Step 5: 更新 `FileIndexContext`**

四个对应公开方法，模式同现有 `query_by_line_range` / `query_last_n`：加锁 → 过期检测 → 获取路径 → 读锁 → 委托 `FileIndices` → `combine_record`。

```rust
pub async fn query_after(&self, key: &K, line: u32, n: u32) -> Result<Vec<(u32, Arc<RR>)>> {
    self.get_lock(key);
    self.ensure_updated(key).await?;
    let file_path = self.parser.get_path(key).await?;
    let lock = self.get_lock(key);
    let indices = lock.read().await;
    let raw = indices.query_after::<R>(file_path, line, n).await?;
    Ok(raw.into_iter().map(|(idx, rec)| (idx, Arc::new(self.parser.combine_record(key, &rec)))).collect())
}
```

- [ ] **Step 6: 更新测试**

```rust
// 原 test_query_by_line_range → test_query_after
#[tokio::test]
async fn test_query_after() {
    // 写10行，query_after(2, 3) → 行2,3,4
}

// 原 test_query_by_line_range_empty → 删除（query_after(2, 0) 返回空）
#[tokio::test]
async fn test_query_after_zero() {
    let results = indexer.query_after(&date.to_string(), 2, 0).await.unwrap();
    assert_eq!(results.len(), 0);
}

// 新 test_query_after_oob
#[tokio::test]
async fn test_query_after_oob() {
    // line >= end_line_index → Err
    let results = indexer.query_after(&date.to_string(), 100, 5).await;
    assert!(results.is_err());
}

// 原 test_query_by_line_range_oob → 改为 test_query_after_short
#[tokio::test]
async fn test_query_after_short() {
    // 写10行, query_after(5, 20) → 5行
}

// 原 test_query_last_n → test_query_before
#[tokio::test]
async fn test_query_before() {
    // 写10行, query_before(9, 3) → 行7,8,9
}

// 原 test_query_last_n_overflow → test_query_before_short
#[tokio::test]
async fn test_query_before_short() {
    // 写5行, query_before(4, 100) → 5行
}

// 新 test_query_before_oob
#[tokio::test]
async fn test_query_before_oob() {
    // line >= end_line_index → Err
}

// 新 test_query_first
#[tokio::test]
async fn test_query_first() {
    // 写10行, query_first(3) → 行0,1,2
}

// 新 test_query_last
#[tokio::test]
async fn test_query_last() {
    // 写10行, query_last(3) → 行7,8,9
}

// 修改 test_incremental_update_line_index: 
// query_by_line_range(0, 1005) → query_after(0, 1005)
// query_last_n(10) → query_last(10)
// query_by_line_range(2000, 2005) → query_after(2000, 5)
```

- [ ] **Step 7: 编译并运行全部测试**

Run: `cargo test --manifest-path /home/admin/project/kissbot/kai-rs/kai-file/Cargo.toml -- --test-threads=1`
Expected: 42 tests PASS

- [ ] **Step 8: Commit**

```bash
git add kai-file/src/index.rs
git commit -m "feat: replace query_by_line_range/query_last_n with query_after/before/first/last"
```
