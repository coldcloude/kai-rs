use std::{collections::{HashMap, HashSet}, hash::Hash};

use crate::{error::Result};

pub struct Split {
    pub index: usize,
    pub start: usize,
}

pub trait Index<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn insert(&mut self, key: &K, contents: impl IntoIterator<Item = Vec<T>>) -> Result<()>;

    /// 查找文档
    /// 只做完全匹配，部分匹配由调用方自行处理
    fn find(&self, query: &[T]) -> HashSet<K>;
}
pub trait IndexRemovable<T,K> : Index<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn remove(&mut self, key: &K, contents: impl IntoIterator<Item = Vec<T>>);
}

pub trait IndexWithDetail<T,K> : Index<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn remove(&mut self, key: &K);

    /// 查找文档
    /// 只做完全匹配，部分匹配由调用方自行处理
    fn find_detail(&self, query: &[T], prefix_only: bool) -> HashMap<K,Vec<Split>>;

    fn retrieve(&self, key: &K, index: usize) -> Result<Vec<T>>;

    fn find(&self, query: &[T]) -> HashSet<K> {
        let mut result_map = self.find_detail(query, false);
        let mut result = HashSet::new();
        for (key, splits) in result_map.drain() {
            if splits.is_empty() {
                result.insert(key);
            }
        }
        result
    }
}
