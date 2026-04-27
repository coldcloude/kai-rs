pub mod error;
pub mod document;
pub mod tokenizer;
pub mod index;

pub mod distinct_index;
pub mod simple_index;
pub mod atomic_index;
pub mod index_search;

pub mod substring_tokenizer;
pub mod substring_index;
pub mod splintr_tokenizer;
pub mod splintr_index;

pub use error::Error;
pub use document::Document;
pub use tokenizer::Tokenizer;
pub use index::Index;
pub use index_search::IndexSearch;
pub use simple_index::SimpleIndex;
pub use atomic_index::AtomicIndex;
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
        let content = "Hello, world!";
        let tokens = tokenizer.tokenize(content);
        assert_eq!(tokens, vec![19923, 14, 2058, 3]);
        let mut contents = content.split_whitespace();
        let tokens = tokenizer.tokenize(contents.next().unwrap());
        assert_eq!(tokens, vec![19923, 14]);
        let tokens = tokenizer.tokenize(contents.next().unwrap()); 
        assert_eq!(tokens, vec![29616, 3]);
    }

    #[test]
    fn test_splintr_untokenize() {
        let tokenizer = SplintrTokenizer::new("deepseek_v3").unwrap();
        let tokens = vec![19923, 14, 2058, 3];
        let content = tokenizer.untokenize(&tokens).unwrap();
        assert_eq!(content, "Hello, world!");
    }

    #[test]
    fn test_substring_tokenizer() {
        let tokenizer = SubstringTokenizer {};
        let content = "Hello, world!";
        let tokens = tokenizer.tokenize(content);
        assert_eq!(tokens.iter().map(|x| x.token.clone()).collect::<Vec<_>>(), vec!["hello", ",", "world", "!"]);
        let recover = tokenizer.untokenize(&tokens).unwrap();
        assert_eq!(recover, "hello,world!");
        let tokens = tokenizer.tokenize(&recover);
        assert_eq!(tokens.iter().map(|x| x.token.clone()).collect::<Vec<_>>(), vec!["hello", ",", "world", "!"]);
    }
}
