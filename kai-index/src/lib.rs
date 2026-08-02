pub mod error;
pub mod document;
pub mod tokenizer;
pub mod index;
pub mod hierarchical_tree;

pub mod distinct_index;
pub mod recursive_index;
pub mod simple_index;
pub mod atomic_index;
pub mod index_search;

pub mod prefix_completion;

pub mod substring_tokenizer;
pub mod substring_index;
pub mod splintr_tokenizer;
pub mod splintr_index;

pub use error::Error;
pub use document::Document;
pub use tokenizer::Tokenizer;
pub use index::{Index, IndexWithDetail, IndexRemovable};
pub use index_search::IndexSearch;
pub use recursive_index::RecursiveIndex;
pub use atomic_index::AtomicIndex;
pub use hierarchical_tree::HierarchicalTree;
pub use simple_index::SimpleIndex;
pub use distinct_index::{build_single_index, DistinctIndex};

pub use prefix_completion::{PrefixCompletion, CompletionResult};

pub use substring_tokenizer::SubstringTokenizer;
pub use substring_index::SubstringIndex;
pub use splintr_tokenizer::SplintrTokenizer;
pub use splintr_index::SplintrIndex;

#[cfg(test)]
mod tests {
    // 引入父模块（即 lib.rs 顶层）的所有内容
    use super::*;

    #[test]
    fn test_splintr_tokenize() {
        let tokenizer = SplintrTokenizer::new("deepseek_v3").unwrap();
        let tokens = tokenizer.tokenize("Hello, world!");
        assert_eq!(tokens, vec![19923, 14, 2058, 3]);
        let tokens = tokenizer.tokenize("hello,world!");
        assert_eq!(tokens, vec![33310, 14, 29616, 3]);
    }

    #[test]
    fn test_splintr_untokenize() {
        let tokenizer = SplintrTokenizer::new("deepseek_v3").unwrap();
        let content = tokenizer.untokenize(&vec![19923, 14, 2058, 3]).unwrap();
        assert_eq!(content, "Hello, world!");
        let content = tokenizer.untokenize(&vec![33310, 14, 29616, 3]).unwrap();
        assert_eq!(content, "hello,world!");
    }

    #[test]
    fn test_substring_tokenizer() {
        let tokenizer = SubstringTokenizer {};
        let tokens = tokenizer.tokenize("Hello, world!");
        assert_eq!(tokens.iter().map(|x| x.token.clone()).collect::<Vec<_>>(), vec!["hello", ",", "world", "!"]);
        let recover = tokenizer.untokenize(&tokens).unwrap();
        assert_eq!(recover, "hello,world!");
        let tokens = tokenizer.tokenize(&recover);
        assert_eq!(tokens.iter().map(|x| x.token.clone()).collect::<Vec<_>>(), vec!["hello", ",", "world", "!"]);
    }

    #[test]
    fn test_distinct_index() {
        let single_index = build_single_index(vec![vec![1,1,1,1,1]], 3);
        single_index.for_each(&mut |x| {
            println!("{:?}", x);
        });
        let mut distinct_index: DistinctIndex<i32, String> = DistinctIndex::new(3);
        let key = String::from("a");
        distinct_index.insert(&key, vec![vec![1,1,1,1,1]]);
        let result = distinct_index.find(&vec![1]);
        assert_eq!(result.contains(&key), true);
        let result = distinct_index.find(&vec![1,1]);
        assert_eq!(result.contains(&key), true);
        let result = distinct_index.find(&vec![1,1,1]);
        assert_eq!(result.contains(&key), true);
        let result = distinct_index.find(&vec![1,1,1,1]);
        assert_eq!(result.contains(&key), false);
        let result = distinct_index.find(&vec![1,1,1,1,1]);
        assert_eq!(result.contains(&key), false);
    }

    #[test]
    fn test_substring_index_single_token() {
        let mut idx: SubstringIndex<String> = SubstringIndex::new(32);
        let key = "agent1".to_string();
        let doc = crate::document::to_document("Alice");
        idx.insert(&key, &doc);
        let results = idx.find_all_keys("Alice", false);
        assert_eq!(results.len(), 1, "should find 1 result for 'Alice'");
        assert_eq!(results[0], "agent1");
    }

    #[test]
    fn test_substring_index_multi_token() {
        let mut idx: SubstringIndex<String> = SubstringIndex::new(32);
        idx.insert(&"k1".to_string(), &crate::document::to_document("Hello world"));
        idx.insert(&"k2".to_string(), &crate::document::to_document("Hello there"));
        let r = idx.find_all_keys("world", false);
        assert_eq!(r.len(), 1, "should find 1 for 'world'");
        assert_eq!(r[0], "k1");
    }

    #[test]
    fn test_substring_index_remove_partial() {
        let mut idx: SubstringIndex<String> = SubstringIndex::new(32);
        idx.insert(&"k1".to_string(), &crate::document::to_document("Alice"));
        idx.insert(&"k2".to_string(), &crate::document::to_document("Alicia"));
        // 移除 k1 后，另一个 key 的分支应保留（回溯只删到 k1 所在子树，不误删整树）
        idx.remove(&"k1".to_string(), &crate::document::to_document("Alice"));
        let r = idx.find_all_keys("Alice", false);
        assert_eq!(r.len(), 0, "should find 0 for 'Alice' after removing k1");
        let r = idx.find_all_keys("Alicia", false);
        assert_eq!(r.len(), 1, "should find 1 for 'Alicia' after removing k1");
        assert_eq!(r[0], "k2");
    }

    #[test]
    fn test_substring_index_remove_shared_leaf() {
        let mut idx: SubstringIndex<String> = SubstringIndex::new(32);
        idx.insert(&"k1".to_string(), &crate::document::to_document("Alice"));
        idx.insert(&"k2".to_string(), &crate::document::to_document("Alice Bob"));
        // 两个 key 共享叶子节点，移除 k1 后 k2 仍在同一叶子中，应保持可查
        idx.remove(&"k1".to_string(), &crate::document::to_document("Alice"));
        let r = idx.find_all_keys("Bob", false);
        assert_eq!(r.len(), 1, "should find 1 for 'Bob' after removing k1");
        assert_eq!(r[0], "k2");
        let r = idx.find_all_keys("Alice", false);
        assert_eq!(r.len(), 1, "should find 1 for 'Alice' after removing k1");
        assert_eq!(r[0], "k2");
    }

    #[test]
    fn test_substring_index_remove_last() {
        let mut idx: SubstringIndex<String> = SubstringIndex::new(32);
        idx.insert(&"k1".to_string(), &crate::document::to_document("Alice"));
        // 移除最后一个 key 后索引为空（原实现在此回溯循环 index 下溢 panic）
        idx.remove(&"k1".to_string(), &crate::document::to_document("Alice"));
        let r = idx.find_all_keys("Alice", false);
        assert_eq!(r.len(), 0, "should find 0 after removing the last key");
    }
}
