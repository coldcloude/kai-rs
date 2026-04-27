use std::{collections::{HashMap, HashSet, LinkedList}, hash::Hash};

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
        //使用栈辅助遍历
        let mut tree_list: LinkedList<&TokenNode<T,K>> = LinkedList::new();
        //加入根
        tree_list.push_back(self);
        while let Some(tree) = tree_list.pop_back() {
            //将叶子节点内容加入结果集
            self.get(result);
            //将子树加入队列
            if let Some(sub_tree_map) = tree.sub_tree_map.as_ref() {
                for sub_tree in sub_tree_map.values() {
                    tree_list.push_back(sub_tree);
                }
            }
        }
    }
}

struct EmptyCheck {
    sub_tree_map_size: usize,
    leaf_set_size: usize,
    should_remove_sub_tree: bool
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
                current_tree = current_tree.sub_tree_map.get_or_insert_with(|| HashMap::new()).entry(token).or_insert_with(|| TokenNode::new());
            }
            //在叶子节点记录文档id
            current_tree.leaf_set.get_or_insert_with(|| HashMap::new()).entry(key.clone()).or_insert_with(|| Vec::new()).push(Split {index, start});
        }
    }

    fn remove_one(&mut self, key: &K, tokens: Vec<T>) {
        //取所有长度不超过max_depth的子串进行索引
        for start in 0..tokens.len() {
            let mut valid_tokens = LinkedList::new();
            for curr in start..std::cmp::min(start + self.max_depth, tokens.len()) {
                valid_tokens.push_back(tokens[curr].clone());
            }
            //记录当前节点，前缀子串和其他子串根不同
            let mut current_tree_or_none = if start == 0 {
                Some(&mut self.prefix_tree)
            } else {
                Some(&mut self.tree)
            };
            //记录当前节点是否要删除
            let mut should_remove = false;
            //记录移除前的子树大小
            let mut check_list: Vec<EmptyCheck> = Vec::new();
            for token in valid_tokens.iter() {
                //未找到子树时自动留空
                if let Some(current_tree) = current_tree_or_none.take() {
                    //记录当前子树大小
                    let sub_tree_map_size = match current_tree.sub_tree_map.as_ref() {
                        Some(sub_tree_map) => sub_tree_map.len(),
                        None => 0,
                    };
                    //记录当前叶子节点大小
                    let leaf_set_size = match current_tree.leaf_set.as_ref() {
                        Some(leaf_set) => leaf_set.len(),
                        None => 0,
                    };
                    //检查当前子树是否要移除
                    should_remove = sub_tree_map_size == 0 && leaf_set_size == 0;
                    //搜索当前子树
                    if let Some(sub_tree_map) = current_tree.sub_tree_map.as_mut() {
                        if let Some(sub_tree) = sub_tree_map.get_mut(&token) {
                            //保存当前子树大小
                            check_list.push(EmptyCheck {
                                sub_tree_map_size: sub_tree_map_size,
                                leaf_set_size: leaf_set_size,
                                should_remove_sub_tree: false
                            });
                            //搜索下一个子树
                            current_tree_or_none = Some(sub_tree);
                        }
                    };
                }
                //无法继续遍历，结束
                if current_tree_or_none.is_none() {
                    break;
                }
            }
            //如果找到叶子节点，先移除叶子节点
            if let Some(current_tree) = current_tree_or_none.as_mut() {
                //检查叶子节点的子树，是否需要移除
                should_remove = match current_tree.sub_tree_map.as_ref() {
                    Some(sub_tree_map) => sub_tree_map.is_empty(),
                    None => true,
                };
                if let Some(leaf_set) = current_tree.leaf_set.as_mut() {
                    //移除叶子节点
                    leaf_set.remove(&key);
                    //如果当前节点的子树和叶子均为空，应该移除当前子树
                    should_remove = should_remove && leaf_set.is_empty();
                }
            }
            //如果最后节点为空，则需要递归移除
            if should_remove {
                //回溯并标记移除
                let mut index = check_list.len()-1;
                while should_remove {
                    let check = &mut check_list[index];
                    //标记移除本节点的这个子树
                    check.should_remove_sub_tree = true;
                    //如果只有这个子树，应该移除当前子树
                    should_remove = check.sub_tree_map_size == 1 && check.leaf_set_size == 0;
                    //回溯上一个子树
                    index -= 1;
                }
                //根据标记，直接移除最上层子树
                let mut current_tree_or_none = if start == 0 {
                    Some(&mut self.prefix_tree)
                } else {
                    Some(&mut self.tree)
                };
                //搜索最上层要移除的子树
                for token in valid_tokens.iter() {
                    if let Some(current_tree) = current_tree_or_none.take() {
                        if let Some(sub_tree_map) = current_tree.sub_tree_map.as_mut() {
                            if check_list[index].should_remove_sub_tree {
                                //移除子树
                                sub_tree_map.remove(token);
                                //无需继续遍历
                                break;
                            }
                            else {
                                //继续遍历下一个子树
                                current_tree_or_none = sub_tree_map.get_mut(token);
                            }
                        }
                    }
                    //无法继续遍历，结束
                    if current_tree_or_none.is_none() {
                        break;
                    }
                }
            }
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
