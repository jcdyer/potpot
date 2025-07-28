use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {

    #[error("other error")]
    Other,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}