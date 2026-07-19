# 分钟级时间索引实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `FileIndexContext.time_index` 的 key 从秒级改为分钟级，同分钟记录合并，查询时正向读取 + 按秒过滤。

**Architecture:** `kai-date` 新增 `as_minute` 截断函数；`kai-file` 中 `update_forward_lines` 的 key 改为 `as_minute(time)`；删除 `query_reverse`，新建 `query_by_time_range` 用 `LineReader` 正向读取 + 秒级时间过滤。

**Tech Stack:** Rust, chrono (kai-date), tokio (kai-file)

## Global Constraints

- 不删除注释
- `as_minute(time)` 取前 16 字符，格式为 `"yyyy-MM-dd HH:mm"`
- `time_index` 的 key 改为 `as_minute(record.time()).to_string()`
- `query_by_time_range` 用 `as_minute(start)..=as_minute(end)` 做 range 查找
- 读取后逐行按 `record.time() >= start && record.time() <= end` 做秒级过滤
- 删除 `query_reverse` 方法
- `query_all` 改为调用 `query_by_time_range`
- `query_by_line_range`、`query_last_n`、`page_index`、`FileIndices` 结构体不变
- 现有测试 `test_mark_and_query_channel` 的行为不能变

---

### Task 1: kai-date 新增 as_minute

**Files:**
- Modify: `kai-date/src/lib.rs`

**Interfaces:**
- Produces: `pub fn as_minute(time: &str) -> &str`

- [ ] **Step 1: 在 `kai-date/src/lib.rs` 中新增 `as_minute` 函数**

```rust
pub fn as_minute(time: &str) -> &str {
    time[..16].as_ref()
}
```

位置：放在 `as_year` 和 `get_internal_dates` 之间。

- [ ] **Step 2: 新增 `as_minute` 测试**

```rust
#[test]
fn test_as_minute() {
    assert_eq!(as_minute("2026-06-24 10:30:45"), "2026-06-24 10:30");
    assert_eq!(as_minute("2026-01-01 00:00:00"), "2026-01-01 00:00");
    assert_eq!(as_minute("2026-12-31 23:59:59"), "2026-12-31 23:59");
}
```

- [ ] **Step 3: 运行测试**

Run: `cargo test --manifest-path /home/admin/project/kissbot/kai-rs/kai-date/Cargo.toml`
Expected: PASS（含原有测试共 7 个）

- [ ] **Step 4: Commit**

```bash
git add kai-date/src/lib.rs
git commit -m "feat(kai-date): add as_minute function truncating to minute level"
```

---

### Task 2: kai-file 分钟级 key + 正向时间查询

**Files:**
- Modify: `kai-file/src/index.rs`

**Interfaces:**
- Consumes: `kai_date::as_minute`
- Produces: `query_by_time_range` 替代 `query_reverse`；`query_all` 返回值不变

- [ ] **Step 1: 导入 `kai_date::as_minute`**

在 `kai-file/src/index.rs` 顶部加入：
```rust
use kai_date::as_minute;
```

- [ ] **Step 2: `update_forward_lines` 中 key 改为分钟级**

将：
```rust
match indices.time_index.entry(record.time().to_string()) {
```
改为：
```rust
match indices.time_index.entry(as_minute(record.time()).to_string()) {
```

- [ ] **Step 3: 删除 `query_reverse` 方法**

删除整个 `async fn query_reverse` 方法（约 40 行）。

- [ ] **Step 4: 新增 `query_by_time_range` 方法**

```rust
async fn query_by_time_range(&self, key: &K, start: &str, end: &str) -> Result<Vec<(u64, Arc<RR>)>> {
    self.get_lock(key);
    self.ensure_updated(key).await?;

    let lock = self.get_lock(key);
    let indices = lock.read().await;

    let minute_start = as_minute(start);
    let minute_end = as_minute(end);
    let entries: Vec<_> = indices.time_index.range(minute_start.to_string()..=minute_end.to_string()).collect();

    let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
        return Ok(Vec::new());
    };
    let start_byte = first.1.start_pos;
    let end_byte = last.1.end_pos;
    let start_line = first.1.start_line_index;
    drop(indices);

    let file_path = self.parser.get_path(key).await?;
    let mut reader = LineReader::new(file_path, Some(start_byte), Some(end_byte)).await?;
    let mut results = Vec::new();
    let mut line_idx = start_line;

    while let Some(line_with_pos) = reader.next_line().await? {
        let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
        if record.time() >= start && record.time() <= end {
            let record = self.parser.combine_record(key, &record);
            results.push((line_idx, Arc::new(record)));
        }
        line_idx += 1;
    }

    Ok(results)
}
```

- [ ] **Step 5: 修改 `query_all` 调用 `query_by_time_range`**

```rust
let results = self.query_by_time_range(&key, start.as_str(), end.as_str()).await?;
```

- [ ] **Step 6: 编译验证**

Run: `cargo build --manifest-path /home/admin/project/kissbot/kai-rs/kai-file/Cargo.toml`
Expected: 编译通过

- [ ] **Step 7: 运行全部测试**

Run: `cargo test --manifest-path /home/admin/project/kissbot/kai-rs/kai-file/Cargo.toml -- --test-threads=1`
Expected: 42 tests PASS

- [ ] **Step 8: Commit**

```bash
git add kai-file/src/index.rs
git commit -m "feat(kai-file): minute-level time index keys with forward reading and second-level filtering"
```
