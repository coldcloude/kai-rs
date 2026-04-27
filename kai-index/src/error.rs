use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Tokenizer error: {0}")]
    TokenizerError(#[from] splintr::TokenizerError),

    #[error("Duplicated document key: {0}")]
    DuplicatedDocumentKey(String),

    #[error("Document content not found: {0} {1}")]
    DocumentContentNotFound(String, usize),
}

pub type Result<T> = std::result::Result<T, Error>;
