# 分钟级时间索引

## 背景

`FileIndexContext.time_index` 当前以秒级时间字符串为 key（`record.time()` 返回值如 `"2026-06-24 10:30:45"`）。对于同一分钟内有多条记录的场景（常见于高吞吐写入），每条记录独占一个 `FilePosition`，导致 BTreeMap 条目膨胀、查询时 range 扫描行数多。

优化方向：将 key 粒度提升到分钟级，同分钟记录合并为一个 `FilePosition`，查询时用分钟级 range 定位，再按秒级时间做后过滤。

## 设计

### 1. `as_minute` 函数

位置：`kai-date/src/lib.rs`

```rust
/// 从完整时间字符串中截取到分钟，格式 yyyy-MM-dd HH:mm
pub fn as_minute(time: &str) -> &str {
    time[..16].as_ref()
}
```

输入格式 `"yyyy-MM-dd HH:mm:ss"`，取前 16 字符得到 `"yyyy-MM-dd HH:mm"`。

边界：调用方保证输入长度 ≥ 16（现有 `record.time()` 返回值固定为 `"yyyy-MM-dd HH:mm:ss"` 格式）。

### 2. 分钟级 key 构建

`update_forward_lines` 中：
```rust
// 原：indices.time_index.entry(record.time().to_string())
// 改：
let minute_key = as_minute(record.time()).to_string();
indices.time_index.entry(minute_key)
```

同一分钟的多条记录共享 key，进入 `Occupied` 分支合并 `FilePosition`：
- `start_pos` = min(已有, 当前行)
- `end_pos` = max(已有, 当前行)
- `start_line_index` = min(已有, 当前行)
- `end_line_index` = max(已有 + 1, 当前行 + 1)

合并后，一个 `FilePosition` 描述整分钟的数据在文件中的字节范围和行号范围。

### 3. time-range 查询（正向读取 + 秒级过滤）

原有 `query_reverse` 方法（内部方法，仅被 `query_all` 调用）替换为正向读取逻辑：

```rust
async fn query_by_time_range(
    &self, key: &K, start: &str, end: &str
) -> Result<Vec<(u64, Arc<RR>)>> {
    self.get_lock(key);
    self.ensure_updated(key).await?;

    let lock = self.get_lock(key);
    let indices = lock.read().await;

    let minute_start = as_minute(start);
    let minute_end = as_minute(end);
    let range = indices.time_index.range(minute_start.to_string()..=minute_end.to_string());
    // collect 是因为 range borrows indices，后面需要释放读锁
    let entries: Vec<_> = range.collect();

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
```

**行号计算**：`start_line_index` 可以从 `FilePosition` 中获得。正向读取时，`line_idx` 从 `start_line_index` 递增，逐行计数。

**秒级过滤**：
```rust
if record.time() >= start && record.time() <= end {
    results.push((line_idx, Arc::new(record)));
}
line_idx += 1;
```

### 4. 数据流

```
query_all(query)
  → parser.parse_query → per-day segments: Vec<(key, (start, end))>
  → for each (key, (start, end)):
      → query_by_time_range(key, start, end)
         → minute_start = as_minute(start)    // "2026-06-24 10:30"
         → minute_end   = as_minute(end)      // "2026-06-24 10:35"
         → time_index.range(minute_start..=minute_end)
            → 获取 start_byte (第一个分钟的起始位置)
            → 获取 end_byte   (最后一个分钟的结束位置)
            → 获取 start_line (第一个分钟的起始行号)
         → LineReader(start_byte, end_byte)
         → 正向逐行:
             line_idx = start_line
             record = parse(line)
             if record.time() in [start, end]:
               results.push((line_idx, record))
             line_idx += 1
         → return Vec<(u64, Arc<RR>)>
```

### 5. 删除内容

- `query_reverse` 方法删除
- `line_index_by_time` 查找逻辑删除
- `LinkedList` 返回类型改为 `Vec`

### 6. 测试

请求：

- `as_minute` 单元测试（kai-date）
- `test_mark_and_query_channel` 行为不变（现有断言应继续通过）
- 新增分钟级 key + 秒级过滤验证

不需要改动：

- `query_by_line_range`（按行号查询，不涉及时间 key）
- `query_last_n`（按行数查询，不涉及时间 key）
- `page_index`（行号索引不受影响）
- `FileIndices` 结构体

### 7. 文件改动

| 文件 | 改动 |
|------|------|
| `kai-date/src/lib.rs` | 新增 `as_minute` 函数 + 测试 |
| `kai-file/src/index.rs` | `update_forward_lines` 中 key 改为 `as_minute`；删除 `query_reverse`，新增 `query_by_time_range`（正向读取+秒级过滤）；`query_all` 改调 `query_by_time_range` |
