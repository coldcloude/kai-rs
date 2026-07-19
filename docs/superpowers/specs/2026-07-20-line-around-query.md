# 行号前后查询 API

## 背景

当前 `FileIndices` 提供 `query_by_line_range(start, end)` 和 `query_last_n(n)` 两个按行号查询的方法。实际使用中常用"从某行开始取 n 行"和"取某行之前的 n 行"模式，两个函数需要合并为一组更灵活的替代。

## 设计

### API

替换 `query_by_line_range` 和 `query_last_n`，新增四个方法：

```rust
/// 从 line 开始（包含 line）正向读取 n 行
pub async fn query_after<R: Record>(
    &self, file_path: impl AsRef<Path>, line: u32, n: u32,
) -> Result<Vec<(u32, R)>>

/// 从 line 开始（包含 line）反向读取 n 行，结果按正序返回
pub async fn query_before<R: Record>(
    &self, file_path: impl AsRef<Path>, line: u32, n: u32,
) -> Result<Vec<(u32, R)>>

/// 开头 n 行 = query_after(0, n)
pub async fn query_first<R: Record>(
    &self, file_path: impl AsRef<Path>, n: u32,
) -> Result<Vec<(u32, R)>>

/// 结尾 n 行 = query_before(end_line_index - 1, n)
pub async fn query_last<R: Record>(
    &self, file_path: impl AsRef<Path>, n: u32,
) -> Result<Vec<(u32, R)>>
```

`FileIndexContext` 同样暴露这四个方法，加锁+过期检测后委托 `FileIndices`，对返回的 `R` 做 `combine_record` + `Arc` 包装返回 `Vec<(u32, Arc<RR>)>`。

### query_after 实现

```
actual_start = line.min(end_line_index)
if n == 0: return empty
page = actual_start / 1000
start_byte = page_index[page] or 0
LineReader 从 start_byte 正向读
line_idx 从 page*1000 开始计数
skip to actual_start, collect up to n lines
```

### query_before 实现

使用 `ReverseLineReader` 从 `line` 行所在 page 的末尾反向读取：

```
if n == 0: return empty
if line >= end_line_index: return Err
page = line / 1000
end_byte = page_index[page+1] or end_pos   // page 的结尾（exclusive）
ReverseLineReader(file_path, start_pos=page_index[page], end_pos=end_byte)
扫描整个 page（反向），收集行号 ≤ line 的行，遇到行号 < line - n + 1 时停止
若 n 行未收满且存在上一页，继续扫描上一页
结果 push_front 保证正序
```

`query_first(0, n)` 直接用 `LineReader` 正向读。
`query_last(n)` = `query_before(end_line_index - 1, n)`。

### 边角处理

- `query_after`：`line >= end_line_index` → 报错
- `query_before`：`line >= end_line_index` → 报错
- 数量不足：收集到的行数 `k < n` 时，返回 `k` 行，不报错
- `n == 0` → 返回空
- `end_line_index == 0` → 返回空（不触发错误）

### 删除

- `query_by_line_range`
- `query_last_n`

### 测试

- `query_after` 正常范围
- `query_after` 超范围钳制
- `query_before` 正常范围
- `query_before` 超范围钳制
- `query_first` 开头 n 行
- `query_last` 结尾 n 行
- 增量更新后 `query_last` 正确

### 文件改动

| 文件 | 改动 |
|------|------|
| `kai-file/src/index.rs` | `FileIndices` 删除 `query_by_line_range` / `query_last_n`，新增四个方法；`FileIndexContext` 对应修改 |
