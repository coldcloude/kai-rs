use std::{collections::{HashMap, HashSet}, hash::Hash};

use crate::{Error, error::Result, hierarchical_tree::{Collection, HierarchicalTree}, index::{Index, IndexWithDetail, Split}};

struct SimpleIndexCollection<K>
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    index_map: HashMap<K,Vec<Split>>,
}

impl<K> Collection<(K,Split),K,HashMap<K,Vec<Split>>> for SimpleIndexCollection<K>
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn new() -> Self {
        Self {
            index_map: HashMap::new(),
        }
    }

    fn insert(&mut self, item: (K,Split)) {
        let (key, split) = item;
        self.index_map.entry(key).or_insert_with(|| Vec::new()).push(split);
    }

    fn remove(&mut self, key: &K) {
        self.index_map.remove(key);
    }

    fn get(&self, result: &mut HashMap<K,Vec<Split>>) {
        for (key, splits) in self.index_map.iter() {
            let result_splits = result.entry(key.clone()).or_insert_with(|| Vec::new());
            for split in splits.iter() {
                result_splits.push(Split { index: split.index, start: split.start });
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.index_map.is_empty()
    }
}

type SimpleIndexTree<T,K> = HierarchicalTree<T,(K,Split),K,HashMap<K,Vec<Split>>,SimpleIndexCollection<K>>;

pub struct SimpleIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    tree: SimpleIndexTree<T,K>,
    prefix_tree: SimpleIndexTree<T,K>,
    documents: HashMap<K,Vec<Vec<T>>>,
    max_depth: usize,
}

impl<T,K> SimpleIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    pub fn new(max_depth: usize) -> Self {
        Self {
            tree: SimpleIndexTree::new(),
            prefix_tree: SimpleIndexTree::new(),
            documents: HashMap::new(),
            max_depth,
        }
    }
}

impl<T,K> Index<T,K> for SimpleIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn insert(&mut self, key: &K, contents: impl IntoIterator<Item = Vec<T>>) {
        let mut tokens_list = Vec::new();
        for (index, content) in contents.into_iter().enumerate() {
            //取所有长度不超过max_depth的子串进行索引
            for start in 0..content.len() {
                let mut valid_tokens = Vec::new();
                for curr in start..std::cmp::min(start + self.max_depth, content.len()) {
                    valid_tokens.push(content[curr].clone());
                }
                //前缀子串使用单独的树
                let tree = if start == 0 {
                    &mut self.prefix_tree
                } else {
                    &mut self.tree
                };
                tree.insert(&mut valid_tokens, (key.clone(), Split { index, start }));
            }
            //保存文档内容用于移除
            tokens_list.push(content);
        }
        self.documents.insert(key.clone(), tokens_list);
    }

    fn find(&self, query: &[T]) -> HashSet<K> {
        IndexWithDetail::find(self, query)
    }
}

impl<T,K> IndexWithDetail<T,K> for SimpleIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn remove(&mut self, key: &K) {
        if let Some(tokens_list) = self.documents.remove(key) {
            //找到对应的文档，并移除其中所有token序列
            for tokens in tokens_list {
                //取所有长度不超过max_depth的子串进行索引
                for start in 0..tokens.len() {
                    let mut valid_tokens = Vec::new();
                    for curr in start..std::cmp::min(start + self.max_depth, tokens.len()) {
                        valid_tokens.push(tokens[curr].clone());
                    }
                    //前缀子串使用单独的树
                    let tree = if start == 0 {
                        &mut self.prefix_tree
                    } else {
                        &mut self.tree
                    };
                    tree.remove(key, &mut valid_tokens);
                }
            }
        }
    }

    fn find_detail(&self, query: &[T], prefix_only: bool) -> HashMap<K,Vec<Split>> {
        let mut result: HashMap<K,Vec<Split>> = HashMap::new();
        let mut trees: Vec<&SimpleIndexTree<T,K>> = Vec::new();
        trees.push(&self.tree);
        if prefix_only {
            trees.push(&self.prefix_tree);
        }
        for tree in trees {
            tree.find(query, &mut result);
        }
        result
    }

    fn retrieve(&self, key: &K, index: usize) -> Result<Vec<T>> {
        if let Some(tokens_list) = self.documents.get(key) {
            if let Some(content) = tokens_list.get(index) {
                return Ok(content.clone());
            }
        }
        Err(Error::DocumentContentNotFound(key.to_string(), index))
    }
}
