use std::{collections::{HashMap, HashSet, LinkedList, hash_map::Entry}, hash::Hash};

use crate::{Error, error::Result, index::{Index, IndexWithDetail, Split}};

struct TokenNode<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    sub_tree_map: Option<HashMap<T,TokenNode<T,K>>>,
    leaf_set: Option<HashMap<K,Vec<Split>>>,
}

impl<T,K> TokenNode<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    pub fn new() -> Self {
        Self {
            sub_tree_map: None,
            leaf_set: None,
        }
    }

    pub fn get_sub_tree(&mut self, token: T) -> &mut Self {
        self.sub_tree_map.get_or_insert_with(|| HashMap::new()).entry(token).or_insert_with(|| Self::new())
    }

    pub fn insert_leaf(&mut self, key: K, index: usize, start: usize) {
        self.leaf_set.get_or_insert_with(|| HashMap::new()).entry(key).or_insert_with(|| Vec::new()).push(Split {index, start});
    }

    pub fn remove(&mut self, key: &K, tokens: &mut LinkedList<T>) {
        match tokens.pop_front() {
            Some(token) => {
                //中间节点
                if let Some(sub_tree_map) = self.sub_tree_map.as_mut() {
                    if let Entry::Occupied(mut sub_tree_entry) = sub_tree_map.entry(token) {
                        //存在token对应的子节点，递归执行子节点移除
                        let sub_tree = sub_tree_entry.get_mut();
                        sub_tree.remove(key, tokens);
                        //如果子节点里没有数据了，移除子节点
                        if sub_tree.leaf_set.is_none() && sub_tree.sub_tree_map.is_none() {
                            sub_tree_entry.remove();
                        }
                    }
                    //如果没有子节点了，移除子节点表
                    if sub_tree_map.is_empty() {
                        self.sub_tree_map = None;
                    }
                }
            }
            None => {
                //叶子节点
                if let Some(leaf_set) = self.leaf_set.as_mut() {
                    //移除叶子
                    leaf_set.remove(&key);
                    //如果叶子节点为空，移除叶子表
                    if leaf_set.is_empty() {
                        self.leaf_set = None;
                    }
                }
            }
        }
    }

    pub fn get(&self, result: &mut HashMap<K,Vec<Split>>) {
        if let Some(leaf_set) = self.leaf_set.as_ref() {
            for (key, splits) in leaf_set.iter() {
                let result_splits = result.entry(key.clone()).or_insert_with(|| Vec::new());
                for split in splits.iter() {
                    result_splits.push(Split { index: split.index, start: split.start });
                }
            }
        }
    }

    fn get_all(&self, result: &mut HashMap<K,Vec<Split>>) {
        //将叶子节点内容加入结果集
        self.get(result);
        //将子树加入队列
        if let Some(sub_tree_map) = self.sub_tree_map.as_ref() {
            for node in sub_tree_map.values() {
                node.get_all(result);
            }
        }
    }
}

pub struct SimpleIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    tree: TokenNode<T,K>,
    prefix_tree: TokenNode<T,K>,
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
            tree: TokenNode::new(),
            prefix_tree: TokenNode::new(),
            documents: HashMap::new(),
            max_depth,
        }
    }

    fn insert_one(&mut self, key: &K, index: usize, content: &Vec<T>) {
        //取所有长度不超过max_depth的子串进行索引
        for start in 0..content.len() {
            let mut valid_tokens = LinkedList::new();
            for curr in start..std::cmp::min(start + self.max_depth, content.len()) {
                valid_tokens.push_back(content[curr].clone());
            }
            //前缀子串单独存
            let mut current_tree = if start == 0 {
                &mut self.prefix_tree
            } else {
                &mut self.tree
            };
            //每个token一个子树
            while let Some(token) = valid_tokens.pop_front() {
                current_tree = current_tree.get_sub_tree(token);
            }
            //在叶子节点记录文档id
            current_tree.insert_leaf(key.clone(), index, start);
        }
    }

    fn remove_one(&mut self, key: &K, tokens: Vec<T>) {
        //取所有长度不超过max_depth的子串进行索引
        for start in 0..tokens.len() {
            let mut valid_tokens = LinkedList::new();
            for curr in start..std::cmp::min(start + self.max_depth, tokens.len()) {
                valid_tokens.push_back(tokens[curr].clone());
            }
            //前缀子串单独存
            if start == 0 {
                self.prefix_tree.remove(key, &mut valid_tokens);
            } else {
                self.tree.remove(key, &mut valid_tokens);
            };
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
            //处理当前tokens
            self.insert_one(key, index, &content);
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
                self.remove_one(key, tokens);
            }
        }
    }

    fn find_detail(&self, query: &[T], prefix_only: bool) -> HashMap<K,Vec<Split>> {
        let mut result: HashMap<K,Vec<Split>> = HashMap::new();
        let mut trees: Vec<&TokenNode<T,K>> = Vec::new();
        trees.push(&self.tree);
        if prefix_only {
            trees.push(&self.prefix_tree);
        }
        for tree in trees {
            let mut matched = true;
            //遍历所有token
            let mut current_tree = tree;
            for token in query {
                //获取当前token对应的子树
                if let Some(sub_tree_map) = current_tree.sub_tree_map.as_ref() {
                    if let Some(sub_tree) = sub_tree_map.get(token) {
                        current_tree = sub_tree;
                    }
                    else {
                        matched = false;
                        break;
                    }
                }
                else {
                    matched = false;
                    break;
                }
            }
            if matched {
                current_tree.get_all(&mut result);
            }
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
