use thiserror::Error;

#[derive(Debug, Error)]
pub enum SquirrelError {
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),

    #[error("actor channel closed unexpectedly")]
    ActorClosed,

    #[error("invalid HLC string: {0}")]
    InvalidHlc(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T, E = SquirrelError> = std::result::Result<T, E>;
