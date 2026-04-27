use std::{collections::{HashMap, HashSet, LinkedList}, hash::Hash};

use crate::hierarchical_tree::{Collection, HierarchicalTree};

pub struct SingleIndexNode<T>
where
    T: Eq + Hash + Clone + 'static,
{
    sub_tree_map: Option<HashMap<T,SingleIndexNode<T>>>,
}

impl<T> SingleIndexNode<T>
where
    T: Eq + Hash + Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            sub_tree_map: None,
        }
    }
}

pub fn build<T>(contents: impl IntoIterator<Item = Vec<T>>, max_depth: usize) -> SingleIndexNode<T>
where
    T: Eq + Hash + Clone + 'static,
{
    let mut root = SingleIndexNode::new();
    for content in contents {
        //取所有长度不超过max_depth的子串进行索引
        for start in 0..content.len() {
            let mut valid_tokens = LinkedList::new();
            for curr in start..std::cmp::min(start + max_depth, content.len()) {
                valid_tokens.push_back(content[curr].clone());
            }
            //前缀子串单独存
            let mut current_tree = &mut root;
            //每个token一个子树
            while let Some(token) = valid_tokens.pop_front() {
                current_tree = current_tree.sub_tree_map.get_or_insert_with(|| HashMap::new()).entry(token).or_insert_with(|| SingleIndexNode::new());
            }
        }
    }
    root
}

struct DistinctIndexCollection<K>
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    key_set: HashSet<K>,
}

impl<K> Collection<K,K,HashSet<K>> for DistinctIndexCollection<K>
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn new() -> Self {
        Self {
            key_set: HashSet::new(),
        }
    }

    fn insert(&mut self, item: K) {
        self.key_set.insert(item);
    }

    fn remove(&mut self, key: &K) {
        self.key_set.remove(key);
    }

    fn get(&self, result: &mut HashSet<K>) {
        for key in self.key_set.iter() {
            result.insert(key.clone());
        }
    }

    fn is_empty(&self) -> bool {
        self.key_set.is_empty()
    }
}

type DistinctIndexTree<T,K> = HierarchicalTree<T,K,K,HashSet<K>,DistinctIndexCollection<K>>;

pub struct DistinctIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    tree: DistinctIndexTree<T,K>,
    max_depth: usize,
}
