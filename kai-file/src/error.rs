use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("File is empty")]
    FileEmpty,

    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    #[error("External error: {0}")]
    ExternalError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Write error: {0}")]
    WriteError(String),
}

pub type Result<T> = std::result::Result<T, Error>;
