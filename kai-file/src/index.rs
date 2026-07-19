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

pub trait RecordCombiner<K,R,RR> {
    fn combine_record(&self, key: &K, record: &R) -> RR;
}

#[derive(Debug, Clone)]
struct FilePosition {
    pub start_pos: u64,
    pub end_pos: u64,
    pub start_line_index: u64,
    pub end_line_index: u64,
}

struct FileIndices {
    /// 时间→位置的索引
    time_index: BTreeMap<String, FilePosition>,
    /// 第 N×1000 行的 start_byte，下标 N 对应行号 N×1000
    page_index: Vec<u64>,
    /// 文件总行数
    line_count: u64,
    /// 上次增量加载结束的字节位置
    last_pos: u64,
    /// 上次增量加载结束时的累计行数
    last_line_index: u64,
}

type FileIndexLock = Arc<RwLock<FileIndices>>;

pub struct FileIndexContext<Q,K,R,RR,P> {
    _marker: PhantomData<(Q,R,RR)>,
    index_map: DashMap<K, FileIndexLock>,
    obsolete_set: DashSet<K>,
    all_obsolete_set: DashSet<K>,
    parser: P,
}

impl<Q,K,R,RR,P> FileIndexContext<Q,K,R,RR,P>
where
    K: Eq + Hash + Clone + Send + Sync,
    R: Record,
    RR: Serialize,
    P: FilePathGenerator<K> + QueryParser<Q,K> + RecordCombiner<K,R,RR>,
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
        self.index_map.entry(key.clone()).or_insert_with(|| {
            self.all_obsolete_set.insert(key.clone());
            self.obsolete_set.insert(key.clone());
            Arc::new(RwLock::new(FileIndices {
                time_index: BTreeMap::new(),
                page_index: Vec::new(),
                line_count: 0,
                last_pos: 0,
                last_line_index: 0,
            }))
        }).clone()
    }

    /// 增量更新：从 last_pos 继续正向读取新行
    async fn update_index(&self, key: &K) -> Result<()> {
        let lock = self.get_lock(key);
        let mut indices = lock.write().await;

        let start_pos = indices.last_pos;
        let mut ln_index = indices.last_line_index;

        let file_path = self.parser.get_path(key).await?;
        let mut reader = LineReader::new(file_path, Some(start_pos), None).await?;
        while let Some(line_with_pos) = reader.next_line().await? {
            let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
            match indices.time_index.entry(record.time().to_string()) {
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
                if page >= indices.page_index.len() {
                    indices.page_index.resize(page + 1, 0);
                }
                indices.page_index[page] = line_with_pos.start_pos;
            }
            ln_index += 1;
        }
        indices.line_count = ln_index;
        indices.last_line_index = ln_index;
        indices.last_pos = reader.current_pos();
        Ok(())
    }

    /// 全量重建：从头扫描，清空所有索引
    async fn update_all_index(&self, key: &K) -> Result<()> {
        let lock = self.get_lock(key);
        let mut indices = lock.write().await;

        indices.time_index.clear();
        indices.page_index.clear();
        indices.line_count = 0;
        indices.last_pos = 0;
        indices.last_line_index = 0;

        let file_path = self.parser.get_path(key).await?;
        let mut reader = LineReader::new(file_path, None, None).await?;
        let mut ln_index = 0u64;
        while let Some(line_with_pos) = reader.next_line().await? {
            let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
            match indices.time_index.entry(record.time().to_string()) {
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
                if page >= indices.page_index.len() {
                    indices.page_index.resize(page + 1, 0);
                }
                indices.page_index[page] = line_with_pos.start_pos;
            }
            ln_index += 1;
        }
        indices.line_count = ln_index;
        indices.last_line_index = ln_index;
        indices.last_pos = reader.current_pos();
        Ok(())
    }

    async fn query_reverse(&self, key: &K, start: &str, end: &str) -> Result<LinkedList<(u64, Arc<RR>)>> {
        let lock = self.get_lock(key);

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
        let indices = lock.read().await;
        let mut start_pos: Option<u64> = None;
        let mut end_pos: Option<u64> = None;
        if let Some((_, position)) = indices.time_index.range(start.to_string()..=end.to_string()).next() {
            start_pos = Some(position.start_pos);
        }
        if let Some((_, position)) = indices.time_index.range(start.to_string()..=end.to_string()).next_back() {
            end_pos = Some(position.end_pos);
        }
        if start_pos.is_some() && end_pos.is_some() {
            let file_path = self.parser.get_path(key).await?;
            let mut reader = ReverseLineReader::new(file_path, start_pos, end_pos).await?;

            let line_index_lookup: BTreeMap<u64, u64> = indices.time_index.range(start.to_string()..=end.to_string())
                .map(|(_, pos)| (pos.start_pos, pos.start_line_index))
                .collect();

            while let Some(line_with_pos) = reader.next_line().await? {
                let record = serde_json::from_str::<R>(line_with_pos.line.as_str())?;
                let record = self.parser.combine_record(key, &record);
                let line_index = line_index_lookup.get(&line_with_pos.start_pos).copied().unwrap_or(0);
                results.push_front((line_index, Arc::new(record)));
            }
        }
        Ok(results)
    }

    pub async fn query_all(&self, query: Q) -> Result<Vec<(u64, Arc<RR>)>> {
        let key_with_range = self.parser.parse_query(query);
        let mut results_list = LinkedList::new();
        for (key, (start, end)) in key_with_range {
            let results = self.query_reverse(&key, start.as_str(), end.as_str()).await?;
            results_list.push_back(results);
        }
        //先计算总数
        let mut len = 0;
        for results in results_list.iter() {
            len += results.len();
        }
        //按总长建结果数组
        let mut results = Vec::with_capacity(len);
        while let Some(mut records) = results_list.pop_front() {
            while let Some(record) = records.pop_front() {
                results.push(record);
            }
        }
        Ok(results)
    }

    pub async fn query_by_line_range(&self, key: &K, start_line: u64, end_line: u64) -> Result<Vec<(u64, Arc<RR>)>> {
        self.get_lock(key); // 确保 key 被初始化

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

        let lock = self.get_lock(key);
        let indices = lock.read().await;
        let line_count = indices.line_count;
        let actual_start = start_line.min(line_count);
        let actual_end = end_line.min(line_count);
        if actual_start >= actual_end {
            return Ok(Vec::new());
        }
        let page = (actual_start / 1000) as usize;
        let start_byte = indices.page_index.get(page).copied().unwrap_or(0);
        drop(indices);

        let file_path = self.parser.get_path(key).await?;
        let mut reader = LineReader::new(file_path, Some(start_byte), None).await?;
        let mut results = Vec::new();
        let mut line_idx = page as u64 * 1000;

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

    pub async fn query_last_n(&self, key: &K, n: u64) -> Result<Vec<(u64, Arc<RR>)>> {
        self.get_lock(key); // 确保 key 被初始化

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

        let lock = self.get_lock(key);
        let line_count = lock.read().await.line_count;

        if n == 0 || line_count == 0 {
            return Ok(Vec::new());
        }

        if n >= line_count {
            return self.query_by_line_range(key, 0, line_count).await;
        }

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use kai_date::{as_date, get_date_time_segments};
    use serde::Deserialize;
    use tokio::fs::*;

    // ========== FileIndexContext: mark + query ==========

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

    impl RecordCombiner<String, TestRecod, TestRecod> for TestRecordParser {
        fn combine_record(&self, _: &String, record: &TestRecod) -> TestRecod {
            record.clone()
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
        let indexer = FileIndexContext::new(parser);
        // query range excludes A (line 0), includes B (line 1)
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1.content.as_str(), "B");

        // write C (in range) after MemoryIndexer created
        append_jsonl(dir.path(), date, r#"{"content":"C","time":"2026-06-24 11:00:00"}"#).await;

        // mark + query — incremental load picks up C (line 2)
        indexer.mark_obsolete(&date.to_string());
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1.content.as_str(), "B");
        assert_eq!(results[1].0, 2);
        assert_eq!(results[1].1.content.as_str(), "C");

        // delete file, write D(before start), E(in range), F(after end)
        remove_file(&file_path).await.unwrap();
        append_jsonl(dir.path(), date, r#"{"content":"D","time":"2026-06-24 08:30:00"}"#).await;
        append_jsonl(dir.path(), date, r#"{"content":"E","time":"2026-06-24 10:30:00"}"#).await;
        append_jsonl(dir.path(), date, r#"{"content":"F","time":"2026-06-24 14:00:00"}"#).await;

        // mark_all — full rebuild, only E in range (line 1)
        indexer.mark_all_obsolete(&date.to_string());
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1.content.as_str(), "E");
    }

    #[tokio::test]
    async fn test_query_by_line_range() {
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

    #[tokio::test]
    async fn test_query_by_line_range_empty() {
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
        let indexer = FileIndexContext::new(parser);

        // 空区间
        let results = indexer.query_by_line_range(&date.to_string(), 3, 3).await.unwrap();
        assert_eq!(results.len(), 0);

        // start > end
        let results = indexer.query_by_line_range(&date.to_string(), 5, 3).await.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_query_by_line_range_oob() {
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

    #[tokio::test]
    async fn test_query_last_n() {
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

    #[tokio::test]
    async fn test_query_last_n_overflow() {
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
        let indexer = FileIndexContext::new(parser);

        // 触发全量更新
        let results = indexer.query_by_line_range(&date.to_string(), 0, 1005).await.unwrap();
        assert_eq!(results.len(), 1005);

        // 验证 page 索引: page 0 (line 0), page 1 (line 1000)
        {
            let lock = indexer.index_map.get(&date.to_string()).unwrap();
            let indices = lock.read().await;
            assert_eq!(indices.page_index.len(), 2);
            assert_eq!(indices.line_count, 1005);
        }

        // 追加10行（总1015）
        for i in 1005..1015 {
            let h = i / 60;
            let m = i % 60;
            append_jsonl(dir.path(), date, &format!(r#"{{"content":"L{}","time":"2026-07-19 {:02}:{:02}:00"}}"#, i, h, m)).await;
        }

        // 增量更新
        indexer.mark_obsolete(&date.to_string());
        let results = indexer.query_last_n(&date.to_string(), 10).await.unwrap();
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
        let results = indexer.query_by_line_range(&date.to_string(), 2000, 2005).await.unwrap();
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
        let indexer = FileIndexContext::new(parser);

        // 触发全量更新
        let results = indexer.query_by_line_range(&date.to_string(), 0, 3005).await.unwrap();
        assert_eq!(results.len(), 3005);

        // page 索引4个: 0,1,2,3
        {
            let lock = indexer.index_map.get(&date.to_string()).unwrap();
            let indices = lock.read().await;
            assert_eq!(indices.page_index.len(), 4);
        }

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
}
