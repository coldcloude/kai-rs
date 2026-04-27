use std::hash::Hash;

use crate::{Document, atomic_index::AtomicIndex, index_search::IndexSearch, substring_tokenizer::{SubstringToken, SubstringTokenizer}};

pub struct SubstringIndex<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync + 'static,
{
    index_search: IndexSearch<SubstringToken,K,SubstringTokenizer,AtomicIndex<SubstringToken,K>>,
}

impl<K> SubstringIndex<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync + 'static,
{
    pub fn new(max_depth: usize) -> Self {
        Self {
            index_search: IndexSearch::new(AtomicIndex::new(max_depth), SubstringTokenizer {}),
        }
    }

    pub fn insert<D:Document>(&mut self, key: &K, doc: &D) {
        self.index_search.insert(key, doc)
    }

    pub fn remove(&mut self, key: &K) {
        self.index_search.remove(key)
    }

    pub fn find_all_keys(&self, query: &str, split: bool) -> Vec<K> {
        self.index_search.find_all_keys(query, split)
    }

    pub fn find_by_prefix(&self, prefix: &str) -> Vec<String> {
        self.index_search.find_by_prefix(prefix)
    }
}
