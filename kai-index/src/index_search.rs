use std::{collections::{HashMap, HashSet, LinkedList, hash_map::Entry}, hash::Hash, marker::PhantomData, ops::Range, rc::Rc};

use crate::{Document, index::{Index, IndexRemovable, IndexWithDetail, Split}, tokenizer::Tokenizer};

pub fn split<T>(tokens: &Vec<T>) -> Vec<Vec<Range<usize>>> {
    let mut result_splits: Vec<Vec<Range<usize>>> = Vec::new();
    let n = tokens.len();
    //空文档，直接返回空结果集
    if n == 0 {
        return Vec::new();
    }
    //按分片数由小到大
    for split_num in 1..=n {
        //栈中每个元素为一种拆分，包括当前已经使用的token数，和已固定的分片
        let mut stack = LinkedList::new();
        //初始还未使用token，也无固定分片
        stack.push_back((0, Vec::new()));
        //取一个没有分完的拆分
        while let Some((current_pos, splits)) = stack.pop_back() {
            //确认已经固定的分片数
            let splits_used = splits.len();
            //只剩最后一片要拆，只能固定拆到结尾；否则就再拆分一片出来
            if splits_used == split_num - 1 {
                let mut result = Vec::with_capacity(split_num);
                let mut last_pos = 0;
                //输出所有已经固定的分片
                for &pos in &splits {
                    result.push(last_pos..pos);
                    last_pos = pos;
                }
                //最后一个分片，只能拆到结尾
                result.push(last_pos..n);
                //将拆分结果加入结果集
                result_splits.push(result);
            }
            else {
                //确认还要再拆几片（不算当前这片）
                let remaining_splits = split_num - splits_used - 1;
                //分片最少有1个token，最大要给每个剩余分片留1个token
                let min_pos = current_pos + 1;
                let max_pos = n - remaining_splits;
                //遍历所有可能的分片位置
                for split_pos in (min_pos..=max_pos).rev() {
                    let mut new_splits = splits.clone();
                    new_splits.push(split_pos);
                    stack.push_back((split_pos, new_splits));
                }
            }
        }
    }
    result_splits
}

type RawResult<K> = HashMap<K,Vec<Split>>;
type ResultIndexMap = HashMap<usize,HashMap<usize,usize>>;
type CombinedResult<K> = HashMap<K,ResultIndexMap>;
type CombinedPriorityResult<K> = HashMap<K,(usize,ResultIndexMap)>;

fn combine_raw_result<K>(result: &mut CombinedResult<K>, raw_result: &mut RawResult<K>, token_len: usize, intersect: bool)
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    //交集模式，先清理不匹配的
    if intersect {
        result.retain(|key, _| raw_result.contains_key(key));
    }
    
    for (key, splits) in raw_result.drain() {
        //交集模式只取已有的
        let index_map_or_none = if intersect {
            result.get_mut(&key)
        } else {
            let index_map = result.entry(key).or_insert(HashMap::new());
            Some(index_map)
        };
        if let Some(index_map) = index_map_or_none {
            for split in splits {
                let start_map = index_map.entry(split.index).or_insert(HashMap::new());
                match start_map.entry(split.start) {
                    Entry::Occupied(mut entry) => {
                        let old_len = *entry.get();
                        *entry.get_mut() = std::cmp::max(old_len, token_len);
                    },
                    Entry::Vacant(entry) => {
                        entry.insert(token_len);
                    },
                }
            }
        }
    }
}

fn combine_priority_result<K>(priority_result: &mut CombinedPriorityResult<K>, result: &mut CombinedResult<K>, priority: usize)
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    for (key, index_map) in result.drain() {
        match priority_result.entry(key) {
            Entry::Occupied(mut entry) => {
                if priority < entry.get().0 {
                    *entry.get_mut() = (priority, index_map);
                }
            },
            Entry::Vacant(entry) => {
                entry.insert((priority, index_map));
            },
        }
    }
}

fn combine_priority_key_map<K>(priority_result: &mut HashMap<K,usize>, result: &mut HashSet<K>, priority: usize)
where
    K: Eq + Hash + Clone + ToString + 'static,
{
    for key in result.drain() {
        match priority_result.entry(key) {
            Entry::Occupied(mut entry) => {
                if priority < *entry.get() {
                    *entry.get_mut() = priority;
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(priority);
            },
        }
    }
}

type TokenSplitMap<T> = HashMap<Rc<Vec<T>>,HashMap<Range<usize>,usize>>;

pub struct IndexSearchContext<T>
where
    T: Eq + Hash + Clone + 'static,
{
    current_split_or_none: Option<TokenSplitMap<T>>,
    split_tokens_list: Vec<TokenSplitMap<T>>,
}

impl<T> IndexSearchContext<T>
where
    T: Eq + Hash + Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            current_split_or_none: None,
            split_tokens_list: Vec::new(),
        }
    }
    
    pub fn add_to_split(&mut self, tokens: Rc<Vec<T>>, range: Range<usize>) {
        let current_split = self.current_split_or_none.get_or_insert(HashMap::new());
        match current_split.get_mut(tokens.as_ref()) {
            Some(map) => {
                match map.entry(range) {
                    Entry::Occupied(mut entry) => {
                        *entry.get_mut() += 1;
                    },
                    Entry::Vacant(entry) => {
                        entry.insert(1);
                    },
                };
            },
            None => {
                let mut new_map = HashMap::new();
                new_map.insert(range, 1);
                current_split.insert(tokens.clone(), new_map);
            },
        }
    }

    pub fn end_split(&mut self) {
        let current_split_or_none = self.current_split_or_none.take();
        self.current_split_or_none = None;
        if let Some(current_split) = current_split_or_none {
            self.split_tokens_list.push(current_split);
        }
    }

    pub fn find<K,IDX>(&self, priority_result: &mut HashMap<K,usize>, index: &IDX)
    where
        K: Eq + Hash + Clone + ToString + 'static,
        IDX: Index<T,K>,
    {
        //对tokens去重
        let mut tokens_set: HashSet<&[T]> = HashSet::new();
        for split in &self.split_tokens_list {
            for (tokens, map) in split {
                for range in map.keys() {
                    tokens_set.insert(&tokens[range.start..range.end]);
                }
            }
        }
        //匹配去重后的每组tokens
        let mut raw_result_map: HashMap<&[T],HashSet<K>> = HashMap::new();
        for tokens in tokens_set {
            let raw_result = index.find(tokens);
            raw_result_map.insert(tokens, raw_result);
        }
        //合并结果
        for split_tokens in &self.split_tokens_list {
            //对每一种拆分方式提取结果，要求拆分出的每个tokens都能匹配到
            let mut priority: usize = 0;
            let mut combine_result = HashSet::new();
            for (tokens, range_map) in split_tokens {
                for (range, count) in range_map {
                    priority += count;
                    match raw_result_map.get_mut(&tokens[range.start..range.end]) {
                        None => {
                            //有一个tokens没有找到，查询失败
                            combine_result.clear();
                            break;
                        },
                        Some(raw_result) => {
                            //合并结果，取交集
                            combine_result.retain(|k| raw_result.contains(k));
                        },
                    }
                }
            }
            //保存到最终结果
            if !combine_result.is_empty() {
                combine_priority_key_map(priority_result, &mut combine_result, priority);
            }
        }
    }
}

pub struct IndexSearch<T,K,TKNZ,IDX>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
    TKNZ: Tokenizer<T>,
    IDX: Index<T,K>,
{
    _marker: PhantomData<(T, K)>,
    index: IDX,
    tokenizer: TKNZ,
}

impl<T,K,TKNZ,IDX> IndexSearch<T,K,TKNZ,IDX>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
    TKNZ: Tokenizer<T>,
    IDX: Index<T,K>,
{
    pub fn new(index: IDX, tokenizer: TKNZ) -> Self {
        Self {
            _marker: PhantomData,
            index,
            tokenizer,
        }
    }

    pub fn insert<D>(&mut self, key: &K, document: &D)
    where
        D: Document,
    {
        let mut tokens_list: Vec<Vec<T>> = Vec::new();
        for content in document.contents() {
            let splits = content.split_whitespace();
            let mut tokens: Vec<T> = Vec::new();
            for split in splits {
                tokens.extend(self.tokenizer.tokenize(split));
            }
            tokens_list.push(tokens);
        }
        self.index.insert(key, tokens_list.into_iter())
    }

    pub fn find_all_keys(&self, query: &str, splitable: bool) -> Vec<K> {
        //找到所有结果
        let mut priority_result = HashMap::new();
        if splitable {
            let tokens = Rc::new(self.tokenizer.tokenize(query));
            let mut context = IndexSearchContext::new();
            let splits = split(&tokens);
            for split in splits {
                for range in split {
                    context.add_to_split(tokens.clone(), range.clone());
                }
                context.end_split();
            }
            //查找所有split组合
            context.find(&mut priority_result, &self.index);
        }
        else {
            let parts = query.split_whitespace();
            let mut context = IndexSearchContext::new();
            for part in parts {
                let tokens = Rc::new(self.tokenizer.tokenize(part));
                let len = tokens.len();
                context.add_to_split(tokens,0..len);
            }
            context.end_split();
            context.find(&mut priority_result, &self.index);
        }

        let mut result = Vec::new();
        for (key, priority) in priority_result.drain() {
            result.push((key,priority));
        }
        //按优先级排序
        result.sort_by(|(_, p1), (_, p2)| p1.cmp(&p2));
        //去掉priority字段，只按顺序返回key
        result
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<K>>()
    }
}

impl<T,K,TKNZ,IDX> IndexSearch<T,K,TKNZ,IDX>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
    TKNZ: Tokenizer<T>,
    IDX: IndexRemovable<T,K>,
{
    pub fn remove_content<D:Document>(&mut self, key: &K, document: &D) {
        let contents = document.contents();
        self.index.remove(key, contents.into_iter().map(|content| self.tokenizer.tokenize(content.as_str())));
    }
}

impl<T,K,TKNZ,IDX> IndexSearch<T,K,TKNZ,IDX>
where
    T: Eq + Hash + Clone + 'static,
    K: Eq + Hash + Clone + ToString + 'static,
    TKNZ: Tokenizer<T>,
    IDX: IndexWithDetail<T,K>,
{

    pub fn remove(&mut self, key: &K) {
        self.index.remove(key);
    }

    pub fn find_by_prefix(&self, prefix: &str) -> Vec<String> {
        //找到前缀的所有结果和优先级
        let tokens = self.tokenizer.tokenize(prefix);
        let mut raw_result = self.index.find_detail(&tokens, true);
        let mut combined_result = HashMap::new();
        combine_raw_result(&mut combined_result, &mut raw_result, tokens.len(), false);
        let mut priority_result = HashMap::new();
        combine_priority_result(&mut priority_result, &mut combined_result, 0);
        //取出结果原文并按优先级排序
        let mut result = Vec::new();
        for (key, (_,mut index_map)) in priority_result.drain() {
            for (index, _) in index_map.drain() {
                if let Ok(tokens) = self.index.retrieve(&key, index) {
                    if let Ok(content) = self.tokenizer.untokenize(tokens.as_slice()) {
                        result.push(content);
                    }
                }
            }
        }
        result
    }
}
