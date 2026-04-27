use std::hash::Hash;

use crate::error::Result;

pub trait Tokenizer<T>
where
    T: Eq + Hash + Clone + 'static,
{
    fn tokenize(&self, content: &str) -> Vec<T>;

    fn untokenize(&self, tokens: &[T]) -> Result<String>;
}
