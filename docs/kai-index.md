# kai-index 倒排索引模块
实现倒排索引功能，支持全文搜索

## 功能
- document.rs 提供文档转换功能，将文档转换为倒排索引的输入格式
- tokenizer.rs 提供分词功能，将文档转换为词元
- index.rs 提供倒排索引基础框架，以支持不同的实现
- index_search.rs 基于index.rs实现全文搜索功能
- recursive_index.rs 使用递归实现搜索和删除的倒排索引，性能受限，但实现相对简洁
- atomic_index.rs 在并发环境下线程安全的倒排索引，仅在插入和删除时需要全局锁
- hierarchical_tree.rs 实现通用的具备插入、删除、搜索功能的级联树结构，用于存储倒排索引
- simple_index.rs 基于级联树的简单倒排索引实现，并发环境下需要读写锁
- distinct_index.rs 基于级联树的去重倒排索引实现，无法找回原文，但节省存储空间，并加速搜索
- substring_tokenizer.rs 提供子串分词功能，将文档转换为子串词元，不区分大小写，不能精确还原原始文档
- substring_index.rs 基于distinct_index.rs和substring_tokenizer.rs实现的子串倒排索引，支持快速、不区分大小写搜索，但无法找回原文，不支持自动补全
- split_tokenizer.rs 提供分词功能，将文档转换为词元，可以精确还原原始文档
- split_index.rs 基于atomic_index.rs和split_tokenizer.rs实现的分词倒排索引，支持全文搜索，持自动补全

## 实现约定

### 术语
- document 文档，待索引的文档，搜索的虽小但未。能拆分为数条content，每个content是一个独立索引，但是搜索时按照文档搜索
- token 词元，文档的每个content最终转换成一个token序列进行存储和索引。

### 搜索机制
- 前缀搜索，query必须和文档任意一条content的前缀匹配
- 完全匹配索引，query按照空格切分后，所有分片必须在可在content中完全包含，按照分片间的连续性排序
- 分词匹配索引，query可按分词结果拆分为多个分片，所有分片必须在可在content中完全包含，按照分片间的连续性排序
