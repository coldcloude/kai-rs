use std::hash::Hash;

use crate::{DistinctIndex, Document, document::AsStr, index_search::IndexSearch, substring_tokenizer::{SubstringToken, SubstringTokenizer}};

pub struct SubstringIndex<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync + 'static,
{
    index_search: IndexSearch<SubstringToken,K,SubstringTokenizer,DistinctIndex<SubstringToken,K>>,
}

impl<K> SubstringIndex<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync + 'static,
{
    pub fn new(max_depth: usize) -> Self {
        Self {
            index_search: IndexSearch::new(DistinctIndex::new(max_depth), SubstringTokenizer {}),
        }
    }

    pub fn insert<D,S>(&mut self, key: &K, doc: &D)
    where
        D: Document<S>,
        S: AsStr,
    {
        self.index_search.insert(key, doc)
    }

    pub fn remove<D,S>(&mut self, key: &K, doc: &D)
    where
        D: Document<S>,
        S: AsStr,
    {
        self.index_search.remove_content(key,doc)
    }

    pub fn find_all_keys(&self, query: &str, split: bool) -> Vec<K> {
        self.index_search.find_all_keys(query, split)
    }
}
