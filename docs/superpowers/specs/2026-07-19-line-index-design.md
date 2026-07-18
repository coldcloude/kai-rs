# FileIndexContext 行号索引设计

## 背景

为 `kai-file` 模块的 `FileIndexContext` 增加按行号（下标）定位和读取的能力，包括：

- 构建每 1000 行的 page 索引
- 按行号区间返回内容
- 返回最后 N 行内容
- 原有 time-range 查询返回值统一携带行号

## 设计

### 1. 新增 `LineReader`（正向读取器）

位置：`kai-file/src/line_reader.rs`

与 `ReverseLineReader` 对称的逐行正向读取器，边界处理行为一致。

```rust
pub struct LineReader {
    file: File,
    start_pos: u64,
    end_pos: u64,
    current_pos: u64,
    buffer: Vec<u8>,
    buffer_len: usize,
    leftover: Vec<u8>,
}
```

#### 构造函数

```rust
pub async fn new(path: impl AsRef<Path>, start_pos: Option<u64>, end_pos: Option<u64>) -> Result<Self>
```

语义与 `ReverseLineReader::new` 完全一致：
- `start_pos` 默认为 0
- `end_pos` 默认为文件大小
- 钳制逻辑相同

#### 核心方法

```rust
pub async fn next_line(&mut self) -> Result<Option<LineWithPosition>>
```

返回的 `LineWithPosition` 中 `start_pos`/`end_pos` 为该行在文件中的 byte 区间（与 ReverseLineReader 一致，`end_pos` 包含换行符）。

#### 读取机制

- 从 `current_pos` 开始，每次读满 `BUFFER_SIZE`(4096)
- 在 buffer 中查找 `\n` 定位行边界
- 跨 buffer 边界的行用 `leftover` 累积
- 遇文件末尾输出剩余数据

#### 边界场景测试覆盖

与 `ReverseLineReader` 测试用例一一对应：

| 测试 | 场景 |
|------|------|
| `test_line_reader_basic` | line 1\nline 2\r\nline 3\nline 4 |
| `test_empty_file` | 空文件 |
| `test_single_line` | 单行无尾换行 |
| `test_ends_with_newline` | line 1\nline 2\n |
| `test_ends_with_crlf` | line 1\r\nline 2\r\n |
| `test_ends_with_double_newline` | line 1\nline 2\n\n |
| `test_ends_with_double_crlf` | line 1\r\nline 2\r\n\r\n |
| `test_with_empty_lines_in_middle` | line 1\n\nline 2\nline 3 |
| `test_only_newline` | \n |
| `test_only_two_newlines` | \n\n |
| `test_custom_start` | 指定 start_pos |
| `test_custom_end` | 指定 end_pos |
| `test_custom_start_end` | 同时指定 |
| `test_line_reader_long_line` | 超 BUFFER_SIZE 的单行（验证跨 buffer） |
| `test_line_reader_chinese` | 中文内容（验证 UTF-8） |

### 2. `FilePosition` 扩展

```rust
struct FilePosition {
    pub start_pos: u64,
    pub end_pos: u64,
    pub start_line_index: u64,   // 本记录的首行在文件中的行下标（0-based）
    pub end_line_index: u64,     // 本记录的末行的下一行下标
}
```

因每个 time 唯一对应一条记录（现有假设），`end_line_index = start_line_index + 1`。

### 3. `FileIndexContext` 新增状态

| 字段 | 类型 | 说明 |
|------|------|------|
| `page_index_map` | `DashMap<K, Arc<RwLock<BTreeMap<u64, u64>>>>` | page_number(N) → 第 N×1000 行的 start_byte |
| `line_count_map` | `DashMap<K, Arc<RwLock<u64>>>` | 文件总行数 |
| `last_pos_map` | `DashMap<K, Arc<RwLock<u64>>>` | 上次正向扫描结束的 byte 偏移 |
| `last_line_index_map` | `DashMap<K, Arc<RwLock<u64>>>` | 上次正向扫描结束时的累计行数 |

`get_lock` 方法中同时初始化这些新字段。

### 4. 索引更新策略（统一改用正向读取）

#### 全量重建（`update_all_index`）

触发条件：`all_obsolete_set` 包含 key。

1. 清空 time BTreeMap、page_index、line_count
2. 重置 last_pos = 0、last_line_index = 0
3. `LineReader::new(file, None, None)` 从头遍历
4. 每行：
   - 解析 record
   - line_index = 当前累计行数
   - 设置 FilePosition 的 line_index 字段
   - 插入 time BTreeMap
   - 若 line_index % 1000 == 0：page_index[page] = start_pos
   - line_count += 1
5. 更新 last_pos = 文件末尾, last_line_index = line_count

#### 增量更新（`update_index`）

触发条件：`obsolete_set` 包含 key（且不在 `all_obsolete_set`）。

1. `LineReader::new(file, Some(last_pos), None)` 从上一次结尾继续
2. 每行：
   - 解析 record，与现有 BTreeMap 中同 time 条目合并或插入（保留原有逻辑）
   - line_index = last_line_index + 当前迭代计数
   - 设置 FilePosition 的 line_index
   - 检查是否跨越新的 page 边界（line_index % 1000 == 0）
   - line_count += 1
3. 更新 last_pos、last_line_index

#### 共享 update 方法

```rust
async fn update_forward(
    guard: RwLockWriteGuard<BTreeMap<String, FilePosition>>,
    page_guard: RwLockWriteGuard<BTreeMap<u64, u64>>,
    line_count_guard: RwLockWriteGuard<u64>,
    last_pos_guard: RwLockWriteGuard<u64>,
    last_line_index_guard: RwLockWriteGuard<u64>,
    file_path: &Path,
    incremental: bool,
) -> Result<()>
```

`incremental = false` 时：先 clear 所有状态，从头扫。
`incremental = true` 时：从已有 `last_pos` 继续。

### 5. 过期间标记逻辑

- `mark_obsolete(key)`：仅设 `obsolete_set`。查询时触发增量更新。
- `mark_all_obsolete(key)`：设 `all_obsolete_set` 和 `obsolete_set`，并且重置 `last_pos = 0`、`last_line_index = 0`。查询时触发全量重建。

### 6. 查询 API

#### `query_reverse`（保留但改为使用正向索引）

```rust
async fn query_reverse(&self, key: &K, start: &str, end: &str) -> Result<LinkedList<(u64, Arc<RR>)>>
```

变化：返回携带行号（`(u64, Arc<RR>)`）。

#### `query_by_line_range(key, start_line: u64, end_line: u64) -> Result<Vec<(u64, Arc<RR>)>>`

1. 检查是否过期 → 触发更新
2. 读取 line_count，钳制范围到 `[0, line_count)`
3. 若 `start_line >= end_line` 或 `start_line >= line_count`，返回空
4. page = `start_line / 1000`
5. start_byte = `page_index.get(page).unwrap_or(0)`
6. `LineReader::new(file, Some(start_byte), None)`
7. 遍历行：
   - `current_line` 从 `page * 1000` 开始计数
   - `current_line >= start_line` 时开始收集
   - `current_line >= end_line` 时停止

#### `query_last_n(key, n: u64) -> Result<Vec<(u64, Arc<RR>)>>`

1. 检查是否过期 → 触发更新
2. 读取 line_count
3. 若 `n >= line_count` → 退化为 `query_by_line_range(key, 0, line_count)`
4. 否则用 `ReverseLineReader` 从文件末尾反向读 n 行
5. 读到的行解析为 record，组合为 `(line_index, Arc<RR>)`
6. 因反向读取，需 push_front 保证正序

#### `query_all`（统一返回格式）

```rust
pub async fn query_all(&self, query: Q) -> Result<Vec<(u64, Arc<RR>)>>
```

现有逻辑不变，但每个 record 包裹 `(line_index, Arc<record>)`。

### 7. 测试策略

#### LineReader 测试

与 ReverseLineReader 的 `#[cfg(test)] mod tests` 一一对应，涵盖全部边界场景。

#### FileIndexContext 现有测试适配

`test_mark_and_query_channel`：
- 断言从 `results[0].content` 改为 `results[0].1.content`
- 增加行号验证：A(行0)、B(行1)、C(行2)、D(行3)、E(行4)、F(行5)

#### 新增测试

- `test_query_by_line_range_normal`：按行号区间读取
- `test_query_by_line_range_empty`：空区间
- `test_query_by_line_range_oob`：超范围钳制
- `test_query_last_n`：取最后 N 行
- `test_query_last_n_overflow`：N > 总行数
- `test_incremental_update_line_index`：追加后增量更新，验证行号连续性
- `test_page_index`：超 1000 行的大文件验证 page 索引

### 8. 单元测试场景详解

#### 8.1 超过1000行的增量和行号连续性

```rust
#[tokio::test]
async fn test_incremental_update_line_index() {
    // 1. 写入1005行 -> line_index 0..1004
    // 2. 创建 FileIndexContext
    // 3. 执行一次查询 -> 触发全量更新, page_index 应有 key 0,1
    //    page 0 -> line 0 的 start_byte
    //    page 1 -> line 1000 的 start_byte
    //    返回 1005 条
    // 4. 再追加 10 行 -> line_index 1005..1014
    // 5. mark_obsolete + 查询 -> 增量更新
    //    总行数变为 1015
    //    验证 page_index 不变（未跨越新 page 边界）
    // 6. 再追加 10 行，验证 1020%1000==0 时 page_index 新增 key 2
    // 7. query_by_line_range(key, 1000, 1020) -> 验证返回正确
    // 8. query_last_n(key, 10) -> 验证返回最后10行
    //    再用 query_by_line_range(key, 1010, 1020) 交叉验证
}
```

#### 8.2 page索引在大文件上的正确性

```rust
#[tokio::test]
async fn test_page_index() {
    // 1. 写入 3005 行
    // 2. 全量构建索引
    // 3. 验证 page_index 有 4 个 key: 0, 1, 2, 3
    //    page 0 -> line 0 的 start_byte
    //    page 1 -> line 1000 的 start_byte
    //    page 2 -> line 2000 的 start_byte
    //    page 3 -> line 3000 的 start_byte
    // 4. query_by_line_range(key, 1500, 2500) -> 验证返回行数和内容
    // 5. query_by_line_range(key, 0, 3005) -> 验证返回全部行
    // 6. query_last_n(key, 5) -> 验证最后5行
}
```

#### 8.3 超范围钳制

```rust
#[tokio::test]
async fn test_query_by_line_range_oob() {
    // 1. 写入 10 行
    // 2. query_by_line_range(key, 5, 15) -> 返回行5..9 (5条)
    // 3. query_by_line_range(key, 10, 20) -> 返回空
    // 4. query_last_n(key, 20) -> 返回全部10行
}
```

## 文件改动一览

| 文件 | 操作 |
|------|------|
| `kai-file/src/line_reader.rs` | 新建 |
| `kai-file/src/lib.rs` | 追加 `pub mod line_reader` 和 `pub use line_reader::LineReader` |
| `kai-file/src/index.rs` | FilePosition 扩展；FileIndexContext 新增四个 DashMap；增删改方法 |

`kai-file/src/reverse_line_reader.rs` 和 `kai-file/src/error.rs` 无需改动。

## 不做的事情（YAGNI）

- 不做复合索引（多个 key 的联合查询）
- 不做 page 索引的持久化（每次重建）
- 不做文件变化的主动监听（依赖调用方 `mark_obsolete`）
