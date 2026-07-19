use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::btree_map::Entry;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockWriteGuard};
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

type FileIndexLock = Arc<RwLock<BTreeMap<String, FilePosition>>>;

pub struct FileIndexContext<Q,K,R,RR,P> {
    _marker: PhantomData<(Q,R,RR)>,
    position_map_map: DashMap<K, FileIndexLock>,
    obsolete_set: DashSet<K>,
    all_obsolete_set: DashSet<K>,
    parser: P,
    page_index_map: DashMap<K, Arc<RwLock<BTreeMap<u64, u64>>>>,
    line_count_map: DashMap<K, Arc<RwLock<u64>>>,
    last_pos_map: DashMap<K, Arc<RwLock<u64>>>,
    last_line_index_map: DashMap<K, Arc<RwLock<u64>>>,
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

    pub fn mark_obsolete(&self, key: &K) {
        self.obsolete_set.insert(key.clone());
    }

    pub async fn mark_all_obsolete(&self, key: &K) {
        self.all_obsolete_set.insert(key.clone());
        self.obsolete_set.insert(key.clone());
        if let Some(entry) = self.last_pos_map.get(key) {
            *entry.value().write().await = 0;
        }
        if let Some(entry) = self.last_line_index_map.get(key) {
            *entry.value().write().await = 0;
        }
    }

    fn get_lock(&self, key: &K) -> FileIndexLock {
        let lock = self.position_map_map.entry(key.clone()).or_insert_with(|| {
            // 新创建的 BTree：标记为需要全量加载
            self.all_obsolete_set.insert(key.clone());
            self.obsolete_set.insert(key.clone());
            Arc::new(RwLock::new(BTreeMap::new()))
        }).clone();
        self.page_index_map.entry(key.clone()).or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())));
        self.line_count_map.entry(key.clone()).or_insert_with(|| Arc::new(RwLock::new(0)));
        self.last_pos_map.entry(key.clone()).or_insert_with(|| Arc::new(RwLock::new(0)));
        self.last_line_index_map.entry(key.clone()).or_insert_with(|| Arc::new(RwLock::new(0)));
        lock
    }

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

            if line_index % 1000 == 0 {
                page_guard.insert(line_index / 1000, line_with_pos.start_pos);
            }

            line_index += 1;
        }

        *line_count_guard = line_index;
        *last_line_index_guard = line_index;
        *last_pos_guard = reader.current_pos();

        Ok(())
    }

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

            let line_index_lookup: BTreeMap<u64, u64> = guard.range(start.to_string()..=end.to_string())
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
        let _position_map = self.get_lock(key);

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

        let actual_start = start_line.min(line_count);
        let actual_end = end_line.min(line_count);
        if actual_start >= actual_end {
            return Ok(Vec::new());
        }

        let page = actual_start / 1000;
        let start_byte = {
            let page_guard = self.page_index_map.get(key).unwrap();
            let pages = page_guard.read().await;
            pages.get(&page).copied().unwrap_or(0)
        };

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

    pub async fn query_last_n(&self, key: &K, n: u64) -> Result<Vec<(u64, Arc<RR>)>> {
        let _position_map = self.get_lock(key);

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

    use std::sync::OnceLock;

    static TEST_DIR: OnceLock<tempfile::TempDir> = OnceLock::new();

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

    /// Initializes the global Config singleton for tests using a temp directory.
    /// Safe to call multiple times — only the first call initializes.
    fn init_test_dir() -> PathBuf {
        TEST_DIR.get_or_init(|| tempfile::tempdir().unwrap()).path().to_path_buf()
    }

    async fn append_jsonl(date: &str, line: &str) {
        let dir = init_test_dir();
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
        let dir = init_test_dir();
        let date = "2026-06-24";
        let file_path = get_path(&dir, date).await.unwrap();
        if file_path.exists() {
            remove_file(&file_path).await.unwrap();
        }

        // timeline: 00:00:00 < A(08:00) < start(09:00) < B(10:00) < C(11:00) < end(13:00) < F(14:00)
        append_jsonl(date, r#"{"content":"A","time":"2026-06-24 08:00:00"}"#).await;
        append_jsonl(date, r#"{"content":"B","time":"2026-06-24 10:00:00"}"#).await;

        let parser = TestRecordParser { root_dir: dir };
        let indexer = FileIndexContext::new(parser);
        // query range excludes A, includes B
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content.as_str(), "B");

        // write C (in range) after MemoryIndexer created
        append_jsonl(date, r#"{"content":"C","time":"2026-06-24 11:00:00"}"#).await;

        // mark + query — incremental load picks up C
        indexer.mark_obsolete(&date.to_string());
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].content.as_str(), "B");
        assert_eq!(results[1].content.as_str(), "C");

        // delete file, write D(before start), E(in range), F(after end)
        remove_file(&file_path).await.unwrap();
        append_jsonl(date, r#"{"content":"D","time":"2026-06-24 08:30:00"}"#).await;
        append_jsonl(date, r#"{"content":"E","time":"2026-06-24 10:30:00"}"#).await;
        append_jsonl(date, r#"{"content":"F","time":"2026-06-24 14:00:00"}"#).await;

        // mark_all — full rebuild, only E in range
        indexer.mark_all_obsolete(&date.to_string()).await;
        let results = indexer.query_all((format!("{} 09:00:00", date), format!("{} 13:00:00", date))).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content.as_str(), "E");
    }
}
