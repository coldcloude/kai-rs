use std::{collections::{HashMap, HashSet, LinkedList}, hash::Hash};

use crate::{Index, hierarchical_tree::{Collection, HierarchicalTree}, index::IndexRemovable};

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

    pub fn for_each<F>(&self, op: &mut F)
    where
        F: FnMut(&mut Vec<T>),
    {
        let mut buffer: LinkedList<T> = LinkedList::new();
        let mut layer_list = LinkedList::new();
        let mut current_node_or_none = Some(self);
        while let Some(current_node) = current_node_or_none.take() {
            match current_node.sub_tree_map.as_ref() {
                Some(map) => {
                    //中间节点，前进一层
                    let mut layer = LinkedList::new();
                    for (token, node) in map.iter() {
                        layer.push_back((token.clone(), node));
                    }
                    layer_list.push_back(layer);
                }
                None => {
                    //叶子节点，添加为tokens
                    if !buffer.is_empty() {
                        //将当前序列作为token添加
                        let mut tokens = Vec::new();
                        for token in buffer.iter() {
                            tokens.push(token.clone());
                        }
                        op(&mut tokens);
                    }
                    //回退一层
                    buffer.pop_back();
                }
            }
            //取出一层
            while let Some(mut layer) = layer_list.pop_back() {
                //取出一个节点
                if let Some((token, node)) = layer.pop_back() {
                    //取到节点，则前进一层
                    buffer.push_back(token);
                    current_node_or_none = Some(node);
                    //将本层放回，以保持层数不会回退
                    layer_list.push_back(layer);
                    //结束本次取值
                    break;
                }
                else {
                    //如果本层为空，回退一层
                    //在根节点会多回退一次，但是不影响
                    buffer.pop_back();
                }
            }
        }
    }
}

pub fn build_single_index<T>(contents: impl IntoIterator<Item = Vec<T>>, max_depth: usize) -> SingleIndexNode<T>
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

impl<T,K> DistinctIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    pub fn new(max_depth: usize) -> Self {
        Self {
            tree: DistinctIndexTree::new(),
            max_depth,
        }
    }
}

impl<T,K> Index<T,K> for DistinctIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn insert(&mut self, key: &K, contents: impl IntoIterator<Item = Vec<T>>) {
        let single_index = build_single_index(contents, self.max_depth);
        single_index.for_each(&mut |tokens| {
            self.tree.insert(tokens, key.clone());
        });
    }

    fn find(&self, query: &[T]) -> HashSet<K> {
        let mut result: HashSet<K> = HashSet::new();
        self.tree.find(query, &mut result);
        result
    }
}

impl<T,K> IndexRemovable<T,K> for DistinctIndex<T,K>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
{
    fn remove(&mut self, key: &K, contents: impl IntoIterator<Item = Vec<T>>) {
        let single_index = build_single_index(contents, self.max_depth);
        single_index.for_each(&mut |tokens| {
            self.tree.remove(key, tokens);
        });
    }
}
