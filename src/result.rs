use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {

    #[error("other error")]
    Other,
}