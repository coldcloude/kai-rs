use thiserror::Error;

use crate::WsMessageUnion;

#[derive(Error, Debug)]
pub enum Error {
    #[error("bin parse error: data too short")]
    BinParse,

    #[error("ws error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("json error error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("send error: {0}")]
    Send(#[from] flume::SendError<WsMessageUnion>),

    #[error("heartbeat handler already started")]
    HeartbeatHandlerAlreadyStarted,
}

pub type Result<T> = std::result::Result<T, Error>;
