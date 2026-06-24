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
}
