use thiserror::Error;

#[derive(Debug, Error)]
pub enum ResyncError {
    #[error("I/O error at path `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to memory-map file `{path}`: {source}")]
    Mmap {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Source path `{0}` does not exist or is not readable")]
    SourceNotFound(String),

    #[error("Destination path `{0}` could not be created")]
    DestCreateFailed(String),

    #[error("Delta application failed for `{0}`")]
    DeltaApplyFailed(String),

    #[error("Channel send error: {0}")]
    ChannelError(String),

    #[error("Thread join error")]
    JoinError,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, ResyncError>;
