use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("schema error: {0}")]
    Schema(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("reducer error: {0}")]
    Reducer(String),

    #[error("rollback error: {0}")]
    Rollback(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    #[error("ack pending: sequence {sequence}, pending for {since:?}")]
    PendingAck { sequence: u64, since: Duration },

    #[error("wrong ack sequence: expected {expected}, got {got}")]
    WrongAckSequence { expected: u64, got: u64 },

    #[error("instance poisoned by previous commit failure ({0}); drop and reopen")]
    Poisoned(String),
}

pub type Result<T> = std::result::Result<T, Error>;
