use std::hash::Hash;

use crate::{PrefixCompletion, CompletionResult, Document, atomic_index::AtomicIndex, document::AsStr, error::Result, index_search::IndexSearch, splintr_tokenizer::SplintrTokenizer};

pub struct SplintrIndex<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync + 'static,
{
    index_search: IndexSearch<u32,K,SplintrTokenizer,AtomicIndex<u32,K>>,
}

impl<K> SplintrIndex<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync + 'static,
{
    pub fn new(tokenizer_name: &str, max_depth: usize) -> Result<Self> {
        let tokenizer = SplintrTokenizer::new(tokenizer_name)?;
        Ok(Self {
            index_search: IndexSearch::new(AtomicIndex::new(max_depth), tokenizer),
        })
    }

    pub fn insert<D,S>(&mut self, key: &K, doc: &D)
    where
        D: Document<S>,
        S: AsStr,
    {
        self.index_search.insert(key, doc)
    }

    pub fn remove(&mut self, key: &K) {
        self.index_search.remove(key)
    }

    pub fn find_all_keys(&self, query: &str, split: bool) -> Vec<K> {
        self.index_search.find_all_keys(query, split)
    }

    pub fn complete(&self, prefix: &str) -> Vec<CompletionResult<K>> {
        self.index_search.complete(prefix)
    }
}
