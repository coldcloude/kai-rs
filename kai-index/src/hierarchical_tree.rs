use std::{collections::{HashMap, LinkedList}, hash::Hash, marker::PhantomData};

pub trait Collection<I,R,V> {
    fn new() -> Self;
    fn insert(&mut self, obj: I);
    fn remove(&mut self, obj: &R);
    fn get(&self, result: &mut V);
    fn is_empty(&self) -> bool;
}

struct EmptyCheck {
    sub_tree_map_size: usize,
    leaf_set_is_empty: bool,
    should_remove_sub_tree: bool
}

struct HierarchicalTreeNode<T,I,R,V,C>
where
    T: Eq + Hash + Clone + 'static,
    C: Collection<I,R,V>,
{
    _marker: PhantomData<(I,R,V)>,
    sub_tree_map: Option<HashMap<T,HierarchicalTreeNode<T,I,R,V,C>>>,
    leaf_set: Option<C>,
}

impl<T,I,R,V,C> HierarchicalTreeNode<T,I,R,V,C>
where
    T: Eq + Hash + Clone + 'static,
    C: Collection<I,R,V>,
{
    fn new() -> Self {
        Self {
            _marker: PhantomData,
            sub_tree_map: None,
            leaf_set: None,
        }
    }

    fn get(&self, result: &mut V) {
        if let Some(leaf_set) = self.leaf_set.as_ref() {
            leaf_set.get(result);
        }
    }

    fn get_all(&self, result: &mut V) {
        //使用栈辅助遍历
        let mut tree_list: LinkedList<&HierarchicalTreeNode<T,I,R,V,C>> = LinkedList::new();
        //加入根
        tree_list.push_back(self);
        while let Some(tree) = tree_list.pop_back() {
            //将叶子节点内容加入结果集
            tree.get(result);
            //将子树加入队列
            if let Some(sub_tree_map) = tree.sub_tree_map.as_ref() {
                for sub_tree in sub_tree_map.values() {
                    tree_list.push_back(sub_tree);
                }
            }
        }
    }
}

pub struct HierarchicalTree<T,I,R,V,C>
where
    T: Eq + Hash + Clone + 'static,
    C: Collection<I,R,V>,
{
    root: HierarchicalTreeNode<T,I,R,V,C>,
}

impl<T,I,R,V,C> HierarchicalTree<T,I,R,V,C>
where
    T: Eq + Hash + Clone + 'static,
    C: Collection<I,R,V>,
{
    pub fn new() -> Self {
        Self {
            root: HierarchicalTreeNode::new(),
        }
    }

    pub fn insert(&mut self, tokens: &mut Vec<T>, item: I) {
        //每个token一个子树
        let mut current_tree = &mut self.root;
        for token in tokens.drain(0..tokens.len()) {
            current_tree = current_tree.sub_tree_map.get_or_insert_with(|| HashMap::new()).entry(token).or_insert_with(|| HierarchicalTreeNode::new());
        }
        //在叶子节点记录文档id
        current_tree.leaf_set.get_or_insert_with(|| C::new()).insert(item);
    }

    pub fn remove(&mut self, record: &R, tokens: &Vec<T>) {
        //记录当前节点，前缀子串和其他子串根不同
        let mut current_tree_or_none = Some(&mut self.root);
        //记录当前节点是否要删除
        let mut should_remove = false;
        //记录移除前的子树大小
        let mut check_list: Vec<EmptyCheck> = Vec::new();
        for token in tokens.iter() {
            //未找到子树时自动留空
            if let Some(current_tree) = current_tree_or_none.take() {
                //记录当前子树大小
                let sub_tree_map_size = match current_tree.sub_tree_map.as_ref() {
                    Some(sub_tree_map) => sub_tree_map.len(),
                    None => 0,
                };
                //记录当前叶子节点大小
                let leaf_set_is_empty = match current_tree.leaf_set.as_ref() {
                    Some(leaf_set) => leaf_set.is_empty(),
                    None => true,
                };
                //检查当前子树是否要移除
                should_remove = sub_tree_map_size == 0 && leaf_set_is_empty;
                //搜索当前子树
                if let Some(sub_tree_map) = current_tree.sub_tree_map.as_mut() {
                    if let Some(sub_tree) = sub_tree_map.get_mut(&token) {
                        //保存当前子树大小
                        check_list.push(EmptyCheck {
                            sub_tree_map_size: sub_tree_map_size,
                            leaf_set_is_empty: leaf_set_is_empty,
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
                leaf_set.remove(record);
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
                should_remove = check.sub_tree_map_size == 1 && check.leaf_set_is_empty;
                //回溯上一个子树
                index -= 1;
            }
            //根据标记，直接移除最上层子树
            let mut current_tree_or_none = Some(&mut self.root);
            //搜索最上层要移除的子树
            for token in tokens.iter() {
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

    pub fn find(&self, query: &[T], result: &mut V) {
        let mut matched = true;
        //遍历所有token
        let mut current_tree = &self.root;
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
            current_tree.get_all(result);
        }
    }
}
