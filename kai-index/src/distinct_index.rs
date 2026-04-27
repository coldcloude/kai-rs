use std::{collections::{HashMap, LinkedList}, hash::Hash};

pub struct DistinctIndex<T>
where
    T: Eq + Hash + Clone + 'static,
{
    sub_tree_map: Option<HashMap<T,DistinctIndex<T>>>,
}

impl<T> DistinctIndex<T>
where
    T: Eq + Hash + Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            sub_tree_map: None,
        }
    }
}

pub fn build<T>(contents: impl IntoIterator<Item = Vec<T>>, max_depth: usize) -> DistinctIndex<T>
where
    T: Eq + Hash + Clone + 'static,
{
    let mut root = DistinctIndex::new();
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
                current_tree = current_tree.sub_tree_map.get_or_insert_with(|| HashMap::new()).entry(token).or_insert_with(|| DistinctIndex::new());
            }
        }
    }
    root
}
