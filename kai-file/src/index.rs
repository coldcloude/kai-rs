use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::btree_map::Entry;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::{BTreeMap, LinkedList};

use kai_date::as_minute;

use crate::error::Result;
use crate::LineReader;
use crate::ReverseLineReader;

pub trait Record: Serialize + DeserializeOwned {
    fn time(&self) -> &str;
}

#[async_trait]
pub trait FilePathGenerator<K> {
    async fn get_path(&self, key: &K) -> Result<PathBuf>;
}

pub trait QueryParser<Q,K> {
    fn parse_query(&self, query: Q) -> Vec<(K, (String, String))>;
}

#[derive(Debug, Clone)]
struct FilePosition {
    pub start_pos: u64,
    pub end_pos: u64,
    pub start_line_index: u32,
    pub end_line_index: u32,
}

struct FileIndices {
    /// 时间→位置的索引
    time_index: BTreeMap<String, FilePosition>,
    /// 第 N×1000 行的 start_byte，下标 N 对应行号 N×1000
    page_index: Vec<u64>,
    /// 文件总行数（也是下次增量加载的起始行号，exclusive）
    end_line_index: u32,
    /// 上次加载结束的末尾字节位置（exclusive，即文件末尾）
    end_pos: u64,
}

impl FileIndices {
    async fn update_forward_lines<R: Record>(&mut self, file_path: impl AsRef<Path>) -> Result<()> {
        let mut reader = LineReader::new(file_path, Some(self.end_pos), None).await?;
        let mut ln_index = self.end_line_index;
        while let Some(line_with_pos) = reader.next_line().await? {
            let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
            match self.time_index.entry(as_minute(record.time()).to_string()) {
                Entry::Occupied(mut entry) => {
                    let min_start = entry.get().start_pos.min(line_with_pos.start_pos);
                    let max_end = entry.get().end_pos.max(line_with_pos.end_pos);
                    let min_line = entry.get().start_line_index.min(ln_index);
                    let max_line = entry.get().end_line_index.max(ln_index + 1);
                    entry.get_mut().start_pos = min_start;
                    entry.get_mut().end_pos = max_end;
                    entry.get_mut().start_line_index = min_line;
                    entry.get_mut().end_line_index = max_line;
                }
                Entry::Vacant(entry) => {
                    entry.insert(FilePosition {
                        start_pos: line_with_pos.start_pos,
                        end_pos: line_with_pos.end_pos,
                        start_line_index: ln_index,
                        end_line_index: ln_index + 1,
                    });
                }
            }
            if ln_index % 1000 == 0 {
                let page = (ln_index / 1000) as usize;
                if page >= self.page_index.len() {
                    self.page_index.resize(page + 1, 0);
                }
                self.page_index[page] = line_with_pos.start_pos;
            }
            ln_index += 1;
        }
        self.end_line_index = ln_index;
        self.end_pos = reader.current_pos();
        Ok(())
    }

    pub async fn update_index<R: Record>(&mut self, file_path: impl AsRef<Path>) -> Result<()> {
        self.update_forward_lines::<R>(file_path).await
    }

    pub async fn update_all_index<R: Record>(&mut self, file_path: impl AsRef<Path>) -> Result<()> {
        self.time_index.clear();
        self.page_index.clear();
        self.end_line_index = 0;
        self.end_pos = 0;
        self.update_forward_lines::<R>(file_path).await
    }

    pub async fn query_by_time_range<R: Record>(
        &self, file_path: impl AsRef<Path>, start: &str, end: &str,
    ) -> Result<Vec<(u32, Arc<R>)>> {
        let minute_start = as_minute(start);
        let minute_end = as_minute(end);
        let entries: Vec<_> = self.time_index.range(minute_start.to_string()..=minute_end.to_string()).collect();

        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(Vec::new());
        };
        let start_byte = first.1.start_pos;
        let end_byte = last.1.end_pos;
        let start_line = first.1.start_line_index;

        let mut reader = LineReader::new(file_path, Some(start_byte), Some(end_byte)).await?;
        let mut results = Vec::new();
        let mut line_idx = start_line;

        while let Some(line_with_pos) = reader.next_line().await? {
            let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
            if record.time() >= start && record.time() <= end {
                results.push((line_idx, Arc::new(record)));
            }
            line_idx += 1;
        }

        Ok(results)
    }

    pub async fn query_after<R: Record>(
        &self, file_path: impl AsRef<Path>, line: u32, n: u32,
    ) -> Result<Vec<(u32, Arc<R>)>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        if line >= self.end_line_index {
            return Err(crate::error::Error::ExternalError(
                format!("line {} out of range, end_line_index={}", line, self.end_line_index).into()
            ));
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
                results.push((line_idx, Arc::new(record)));
            }
            line_idx += 1;
        }

        Ok(results)
    }

    pub async fn query_before<R: Record>(
        &self, file_path: impl AsRef<Path>, line: u32, n: u32,
    ) -> Result<Vec<(u32, Arc<R>)>> {
        if n == 0 || self.end_line_index == 0 {
            return Ok(Vec::new());
        }
        if line >= self.end_line_index {
            return Err(crate::error::Error::ExternalError(
                format!("line {} out of range, end_line_index={}", line, self.end_line_index).into()
            ));
        }

        let page = (line / 1000) as usize;
        let end_byte = self.page_index.get(page + 1).copied().unwrap_or(self.end_pos);

        let mut reader = ReverseLineReader::new(&file_path, None, Some(end_byte)).await?;
        let mut results = LinkedList::new();
        let mut cur_line = self.end_line_index.min((page as u32 + 1) * 1000);

        while let Some(line_with_pos) = reader.next_line().await? {
            cur_line -= 1;
            if cur_line <= line {
                let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
                results.push_front((cur_line, Arc::new(record)));
                if results.len() as u32 >= n {
                    break;
                }
            }
        }

        Ok(results.into_iter().collect())
    }

    pub async fn query_first<R: Record>(
        &self, file_path: impl AsRef<Path>, n: u32,
    ) -> Result<Vec<(u32, Arc<R>)>> {
        self.query_after::<R>(file_path, 0, n).await
    }

    pub async fn query_last<R: Record>(
        &self, file_path: impl AsRef<Path>, n: u32,
    ) -> Result<Vec<(u32, Arc<R>)>> {
        if n == 0 || self.end_line_index == 0 {
            return Ok(Vec::new());
        }
        self.query_before::<R>(file_path, self.end_line_index - 1, n).await
    }
}

type FileIndexLock = Arc<RwLock<FileIndices>>;

pub struct FileIndexContext<Q,K,R,P> {
    _marker: PhantomData<(Q,R)>,
    index_map: DashMap<K, FileIndexLock>,
    obsolete_set: DashSet<K>,
    all_obsolete_set: DashSet<K>,
    pub parser: P,
}

impl<Q,K,R,P> FileIndexContext<Q,K,R,P>
where
    K: Eq + Hash + Clone + Send + Sync,
    R: Record,
    P: FilePathGenerator<K> + QueryParser<Q,K>,
{
    pub fn new(parser: P) -> Self {
        Self {
            _marker: PhantomData,
            index_map: DashMap::new(),
            obsolete_set: DashSet::new(),
            all_obsolete_set: DashSet::new(),
            parser,
        }
    }

    pub fn mark_obsolete(&self, key: &K) {
        self.obsolete_set.insert(key.clone());
    }

    pub fn mark_all_obsolete(&self, key: &K) {
        self.all_obsolete_set.insert(key.clone());
        self.obsolete_set.insert(key.clone());
    }

    fn get_lock(&self, key: &K) -> FileIndexLock {
        // 确保 key 对应的索引已初始化（首次使用时标记为过期）
        self.index_map.entry(key.clone()).or_insert_with(|| {
            self.all_obsolete_set.insert(key.clone());
            self.obsolete_set.insert(key.clone());
            Arc::new(RwLock::new(FileIndices {
                time_index: BTreeMap::new(),
                page_index: Vec::new(),
                end_line_index: 0,
                end_pos: 0,
            }))
        }).clone()
    }

    /// 确保索引已更新
    async fn ensure_updated(&self, key: &K) -> Result<()> {
        // 确保索引已建立
        self.get_lock(key);
        // 检查是否需要更新索引
        if self.obsolete_set.remove(key).is_some() {
            // 如果更新完成后有新的全量变更标记
            if self.all_obsolete_set.remove(key).is_some() {
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
        Ok(())
    }

    /// 增量更新：从 end_pos 继续正向读取新行
    async fn update_index(&self, key: &K) -> Result<()> {
        let file_path = self.parser.get_path(key).await?;
        let lock = self.get_lock(key);
        let mut indices = lock.write().await;
        indices.update_index::<R>(file_path).await
    }

    /// 全量重建：从头扫描，清空所有索引
    async fn update_all_index(&self, key: &K) -> Result<()> {
        let file_path = self.parser.get_path(key).await?;
        let lock = self.get_lock(key);
        let mut indices = lock.write().await;
        indices.update_all_index::<R>(file_path).await
    }

    async fn query_by_time_range(&self, key: &K, start: &str, end: &str) -> Result<Vec<(u32, Arc<R>)>> {
        self.ensure_updated(key).await?;
        let file_path = self.parser.get_path(key).await?;
        let lock = self.get_lock(key);
        let indices = lock.read().await;
        indices.query_by_time_range::<R>(file_path, start, end).await
    }

    pub async fn query_all(&self, query: Q) -> Result<Vec<(K, Vec<(u32, Arc<R>)>)>> {
        let mut key_with_range = self.parser.parse_query(query);
        let mut results = Vec::new();
        for (key, (start, end)) in key_with_range.drain(..) {
            let part = self.query_by_time_range(&key, start.as_str(), end.as_str()).await?;
            results.push((key,part));
        }
        Ok(results)
    }

    pub async fn query_after(&self, key: &K, line: u32, n: u32) -> Result<Vec<(u32, Arc<R>)>> {
        self.ensure_updated(key).await?;
        let file_path = self.parser.get_path(key).await?;
        let lock = self.get_lock(key);
        let indices = lock.read().await;
        indices.query_after::<R>(file_path, line, n).await
    }

    pub async fn query_before(&self, key: &K, line: u32, n: u32) -> Result<Vec<(u32, Arc<R>)>> {
        self.ensure_updated(key).await?;
        let file_path = self.parser.get_path(key).await?;
        let lock = self.get_lock(key);
        let indices = lock.read().await;
        indices.query_before::<R>(file_path, line, n).await
    }

    pub async fn query_first(&self, key: &K, n: u32) -> Result<Vec<(u32, Arc<R>)>> {
        self.ensure_updated(key).await?;
        let file_path = self.parser.get_path(key).await?;
        let lock = self.get_lock(key);
        let indices = lock.read().await;
        indices.query_first::<R>(file_path, n).await
    }

    pub async fn query_last(&self, key: &K, n: u32) -> Result<Vec<(u32, Arc<R>)>> {
        self.ensure_updated(key).await?;
        let file_path = self.parser.get_path(key).await?;
        let lock = self.get_lock(key);
        let indices = lock.read().await;
        indices.query_last::<R>(file_path, n).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kai_date::{as_date, get_date_time_segments};
    use serde::Deserialize;
    use tokio::fs::*;

    // ========== FileIndexContext: mark + query ==========

    type TestContext = FileIndexContext<(String, String), String, TestRecod, TestRecordParser>;

    async fn get_path(dir: impl AsRef<Path>, key: &str) ->  Result<PathBuf> {
        let dir_ref = dir.as_ref();
        if !dir_ref.exists() {
            create_dir_all(dir_ref).await?;
        }
        Ok(dir_ref.join(format!("{}.jsonl",key)))
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestRecod {
        time: Arc<String>,
        content: Arc<String>
    }

    impl Record for TestRecod {
        fn time(&self) -> &str {
            self.time.as_str()
        }
    }

    struct TestRecordParser {
        root_dir: PathBuf
    }

    #[async_trait]
    impl FilePathGenerator<String> for TestRecordParser {
        async fn get_path(&self, key: &String) ->  Result<PathBuf> {
            get_path(self.root_dir.as_path(), key).await
        }
    }

    impl QueryParser<(String,String),String> for TestRecordParser {
        fn parse_query(&self, query: (String, String)) -> Vec<(String,(String,String))> {
            let mut results = Vec::new();
            if let Ok(date_times) = get_date_time_segments(&query.0, &query.1) {
                for time in date_times {
                    let date = as_date(&time.0);
                    results.push((date.to_string(), time));
                }
            }
            results
        }
    }

    async fn append_jsonl(dir: impl AsRef<Path>, date: &str, line: &str) {
        let path = get_path(dir, date).await.unwrap();
        let mut f = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .await
            .unwrap();
        use tokio::io::AsyncWriteExt;
        f.write_all(line.as_bytes()).await.unwrap();
        f.write_all(b"\n").await.unwrap();
        f.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_mark_and_query_channel() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-06-24";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        // timeline: 00:00:00 < A(08:00) < start(09:00) < B(10:00) < C(11:00) < end(13:00) < F(14:00)
        append_jsonl(dir.path(), date, r#"{"content":"A","time":"2026-06-24 08:00:00"}"#).await;
        append_jsonl(dir.path(), date, r#"{"content":"B","time":"2026-06-24 10:00:00"}"#).await;

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);
        // query range excludes A (line 0), includes B (line 1)
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].0, 1);
        assert_eq!(results[0].1[0].1.content.as_str(), "B");

        // write C (in range) after MemoryIndexer created
        append_jsonl(dir.path(), date, r#"{"content":"C","time":"2026-06-24 11:00:00"}"#).await;

        // mark + query — incremental load picks up C (line 2)
        indexer.mark_obsolete(&date.to_string());
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.len(), 2);
        assert_eq!(results[0].1[0].0, 1);
        assert_eq!(results[0].1[0].1.content.as_str(), "B");
        assert_eq!(results[0].1[1].0, 2);
        assert_eq!(results[0].1[1].1.content.as_str(), "C");

        // delete file, write D(before start), E(in range), F(after end)
        remove_file(&file_path).await.unwrap();
        append_jsonl(dir.path(), date, r#"{"content":"D","time":"2026-06-24 08:30:00"}"#).await;
        append_jsonl(dir.path(), date, r#"{"content":"E","time":"2026-06-24 10:30:00"}"#).await;
        append_jsonl(dir.path(), date, r#"{"content":"F","time":"2026-06-24 14:00:00"}"#).await;

        // mark_all — full rebuild, only E in range (line 1)
        indexer.mark_all_obsolete(&date.to_string());
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].0, 1);
        assert_eq!(results[0].1[0].1.content.as_str(), "E");
    }

    #[tokio::test]
    async fn test_query_after() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..10 {
            let h = 8 + i;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        let results = indexer.query_after(&date.to_string(), 2, 3).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 2);
        assert_eq!(results[0].1.content.as_str(), "L2");
        assert_eq!(results[1].0, 3);
        assert_eq!(results[1].1.content.as_str(), "L3");
        assert_eq!(results[2].0, 4);
        assert_eq!(results[2].1.content.as_str(), "L4");
    }

    #[tokio::test]
    async fn test_query_after_zero() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..5 {
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, 8+i)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        // n=0 返回空
        let results = indexer.query_after(&date.to_string(), 2, 0).await.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_query_after_short() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..10 {
            let h = 8 + i;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        // n 超出可用范围 → 返回实际可用行数
        let results = indexer.query_after(&date.to_string(), 5, 20).await.unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, 5);
        assert_eq!(results[4].0, 9);
    }

    #[tokio::test]
    async fn test_query_after_oob() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..10 {
            let h = 8 + i;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        // line >= end_line_index → Err
        let results = indexer.query_after(&date.to_string(), 10, 5).await;
        assert!(results.is_err());
    }

    #[tokio::test]
    async fn test_query_before() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..10 {
            let h = 8 + i;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        let results = indexer.query_before(&date.to_string(), 9, 3).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 7);
        assert_eq!(results[0].1.content.as_str(), "L7");
        assert_eq!(results[1].0, 8);
        assert_eq!(results[1].1.content.as_str(), "L8");
        assert_eq!(results[2].0, 9);
        assert_eq!(results[2].1.content.as_str(), "L9");
    }

    #[tokio::test]
    async fn test_query_before_short() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..5 {
            let h = 8 + i;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        // n > 总行数 → 返回全部
        let results = indexer.query_before(&date.to_string(), 4, 100).await.unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, 0);
        assert_eq!(results[4].0, 4);

        // n = 0
        let results = indexer.query_before(&date.to_string(), 4, 0).await.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_query_before_oob() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..10 {
            let h = 8 + i;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        // line >= end_line_index → Err
        let results = indexer.query_before(&date.to_string(), 10, 5).await;
        assert!(results.is_err());
    }

    #[tokio::test]
    async fn test_query_first() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..10 {
            let h = 8 + i;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        let results = indexer.query_first(&date.to_string(), 3).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0);
        assert_eq!(results[0].1.content.as_str(), "L0");
        assert_eq!(results[1].0, 1);
        assert_eq!(results[1].1.content.as_str(), "L1");
        assert_eq!(results[2].0, 2);
        assert_eq!(results[2].1.content.as_str(), "L2");
    }

    #[tokio::test]
    async fn test_query_last() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        for i in 0..10 {
            let h = 8 + i;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:00:00"}}"#, i, h)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        let results = indexer.query_last(&date.to_string(), 3).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 7);
        assert_eq!(results[0].1.content.as_str(), "L7");
        assert_eq!(results[1].0, 8);
        assert_eq!(results[1].1.content.as_str(), "L8");
        assert_eq!(results[2].0, 9);
        assert_eq!(results[2].1.content.as_str(), "L9");
    }

    #[tokio::test]
    async fn test_incremental_update_line_index() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        // 写入1005行
        for i in 0..1005 {
            let h = i / 60;
            let m = i % 60;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        // 触发全量更新
        let results = indexer.query_after(&date.to_string(), 0, 1005).await.unwrap();
        assert_eq!(results.len(), 1005);

        // 验证 page 索引: page 0 (line 0), page 1 (line 1000)
        {
            let lock = indexer.index_map.get(&date.to_string()).unwrap();
            let indices = lock.read().await;
            assert_eq!(indices.page_index.len(), 2);
            assert_eq!(indices.end_line_index, 1005);
        }

        // 追加10行（总1015）
        for i in 1005..1015 {
            let h = i / 60;
            let m = i % 60;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
        }

        // 增量更新
        indexer.mark_obsolete(&date.to_string());
        let results = indexer.query_last(&date.to_string(), 10).await.unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0, 1005);
        assert_eq!(results[9].0, 1014);

        // page 索引不变（未跨越新page边界，1014 < 2000）
        {
            let lock = indexer.index_map.get(&date.to_string()).unwrap();
            let indices = lock.read().await;
            assert_eq!(indices.page_index.len(), 2);
        }

        // 继续追加到跨越2000边界（共2005行）
        for i in 1015..2005 {
            let h = i / 60;
            let m = i % 60;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
        }

        indexer.mark_obsolete(&date.to_string());
        let results = indexer.query_after(&date.to_string(), 2000, 5).await.unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, 2000);

        // page 索引新增 page 2
        {
            let lock = indexer.index_map.get(&date.to_string()).unwrap();
            let indices = lock.read().await;
            assert_eq!(indices.page_index.len(), 3);
        }
    }

    #[tokio::test]
    async fn test_page_index() {
        let dir = tempfile::tempdir().unwrap();
        let date = "2026-07-19";
        let file_path = get_path(dir.path(), date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        // 3005行
        for i in 0..3005 {
            let h = i / 60;
            let m = i % 60;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
        }

        let parser = TestRecordParser { root_dir: dir.path().to_path_buf() };
        let indexer: TestContext = FileIndexContext::new(parser);

        // 触发全量更新
        let results = indexer.query_after(&date.to_string(), 0, 3005).await.unwrap();
        assert_eq!(results.len(), 3005);

        // page 索引4个: 0,1,2,3
        {
            let lock = indexer.index_map.get(&date.to_string()).unwrap();
            let indices = lock.read().await;
            assert_eq!(indices.page_index.len(), 4);
        }

        // 跨页范围查询
        let results = indexer.query_after(&date.to_string(), 1500, 1000).await.unwrap();
        assert_eq!(results.len(), 1000);
        assert_eq!(results[0].0, 1500);
        assert_eq!(results[999].0, 2499);

        // 最后几行
        let results = indexer.query_last(&date.to_string(), 5).await.unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, 3000);
        assert_eq!(results[4].0, 3004);
    }
}
