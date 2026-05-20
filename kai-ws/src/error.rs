use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("bin parse error: {0}")]
    BinParse(#[from] std::array::TryFromSliceError),

    #[error("ws error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("json error error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
