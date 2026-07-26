# kai-file 文件操作模块
提供通用的文件操作功能

## 功能
- reverse_line_reader.rs 提供基于tokio的异步功能，按行倒序读取文本文件内容
  - 每行为正序，行与行之间为倒序
  - O(1)时间复杂度、O(1)空间复杂度
- line_reader.rs 提供基于tokio的异步功能，按行正序读取文本文件内容
  - 支持指定起止字节位置，从文件任意位置开始读取
  - 返回每行的字节位置（start_pos / end_pos），供索引模块定位使用
- appender.rs 提供按 key 分组的批量追加框架（FileObjectAppender）
  - 调用方按 key 追加记录，框架按"超时或批量大小"触发异步落盘
  - 实际的文件写入逻辑由调用方通过 trait 注入，与传输协议、文件格式无关
- index.rs 提供 jsonl 文件索引功能（FileIndexContext）
  - 为按时间组织的 jsonl 记录文件建立内存索引
  - 支持按时间范围、按行号（向前/向后/首/尾）快速查询

## 实现约定

### 术语
- 行：以换行符(\n)分隔的文本单元，支持Windows风格的\r\n
- 缓冲区：用于从文件末尾向前读取数据的临时存储区
- 记录（record）：jsonl 文件中的一行，反序列化为一个对象
- key：文件标识，索引与追加均按 key 管理多个文件

### 倒序行读取机制（reverse_line_reader）
- 从文件末尾开始，以固定大小的缓冲区向前读取
- 在缓冲区中查找换行符来定位行边界
- 按倒序返回行内容，每行内部保持正序
- 支持流式读取，不需要将整个文件加载到内存

### 正序行读取机制（line_reader）
- 构造时可指定 start_pos / end_pos（均为字节偏移，end_pos 为 exclusive），缺省读取整个文件
- 以固定大小缓冲区顺序读取，跨缓冲区的行通过 leftover 暂存拼接
- 末尾无换行符的最后一行也作为一行返回
- 每行返回 LineWithPosition（line、start_pos、end_pos），字节位置与倒序读取一致，可交叉使用

### 批量追加机制（appender）
- FileObjectAppender 按 key 管理一组追加缓冲区，append() 只写入内存队列后立即返回
- 每个 key 首次 append 时创建缓冲区并启动 flush 任务，满足任一条件即触发落盘：
  - 累计记录数达到 batch_size（通过 Notify 唤醒）
  - 距缓冲区创建超过 timeout
- flush 时通过 writer.get_lock(key) 获取该 key 的写入上下文锁，排空队列合并为一个批次，调用上下文 write() 落盘；落盘完成后移除缓冲区和锁
- flush 期间到达的 append 因缓冲区已移除而自动创建新缓冲区重试，不会丢数据
- 写入失败时回调 ErrorHandler（携带 key、整批记录和错误），默认 NoopErrorHandler 忽略
- 调用方需实现两组 trait：
  - FileAppendWriter：按 key 提供/回收写入上下文锁（get_lock / remove_lock）
  - FileAppendWriterContext：执行实际的批量写入（write）
- 使用方既可以是本地 jsonl 文件（channel-web 消息存储），也可以是远程 HTTP 推送（channel 的 MemoryStoreClient）

### 文件索引机制（index）
- 面向"每行一条带 time 字段的 jsonl 记录"的文件，记录类型需实现 Record trait（Serialize + DeserializeOwned + time()）
- 每个 key 维护两份索引：
  - 时间索引：分钟粒度（as_minute）的 BTreeMap，记录该分钟覆盖的字节范围和行号范围
  - 页索引：每 1000 行记录起始字节位置，用于按行号定位
- 懒加载更新：
  - mark_obsolete(key)：标记增量过期，下次查询时从上次结束位置（end_pos）继续正序读取新行补充索引
  - mark_all_obsolete(key)：标记全量过期（文件被重排/重建），下次查询时清空索引从头扫描
  - 更新失败时恢复过期标记，下次查询重试
- 查询能力：
  - query_all(query)：由 QueryParser 将外部查询解析为多个 (key, 时间范围)，按时间索引定位字节范围后用 LineReader 精读过滤
  - query_after / query_before：按行号向前/向后翻页，借助页索引定位到最近的 1000 行边界再读
  - query_first / query_last：取首/尾 N 行
  - 所有查询返回 (行号, Arc<记录>)，行号为文件内的全局行号（从 0 开始）
- 调用方需提供 FilePathGenerator（key → 文件路径）和 QueryParser（查询 → key + 时间范围）
