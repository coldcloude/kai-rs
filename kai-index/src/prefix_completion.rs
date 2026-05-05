use std::{hash::Hash};
use dashmap::{DashMap, DashSet};

use crate::document::{AsStr, Document};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionResult<K>
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    pub completion: String,
    pub key: K,
}

pub trait PrefixCompletion<K>
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn complete(&self, prefix: &str) -> Vec<CompletionResult<K>>;
}

pub struct SimplePrefixCompletion<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync  + 'static,
{
    prefix_map: DashMap<String, DashMap<K, DashSet<String>>>,
}

impl<K> SimplePrefixCompletion<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync  + 'static,
{
    pub fn new() -> Self {
        Self {
            prefix_map: DashMap::new(),
        }
    }

    pub fn insert<D,S>(&self, key: &K, document: &D)
    where 
        D: Document<S>,
        S: AsStr,
    {
        for content in document.contents() {
            let mut prefix = String::new();
            let content = content.as_string();
            for c in content.chars() {
                prefix.push(c);
                self.prefix_map.entry(prefix.clone()).or_insert_with(|| DashMap::new()).entry(key.clone()).or_insert_with(|| DashSet::new()).insert(content.to_string());
            }
        }
    }

    pub fn remove<D,S>(&self, key: &K, document: &D)
    where 
        D: Document<S>,
        S: AsStr,
    {
        for content in document.contents() {
            let mut prefix = String::new();
            let content = content.as_string();
            for c in content.chars() {
                prefix.push(c);
                let prefix = prefix.clone();
                if let Some(map) = self.prefix_map.get(&prefix) {
                    map.remove(key);
                }
            }
        }
    }
}

impl<K> PrefixCompletion<K> for SimplePrefixCompletion<K>
where
    K: Eq + Hash + Clone + ToString + Send + Sync  + 'static,
{
    fn complete(&self, prefix: &str) -> Vec<CompletionResult<K>> {
        let mut results = Vec::new();
        if let Some(map) = self.prefix_map.get(prefix) {
            for entry in map.iter() {
                for content in entry.value().iter() {
                    results.push(CompletionResult {
                        completion: content.clone(),
                        key: entry.key().clone().into(),
                    });
                }
            }
        }
        results
    }
}
