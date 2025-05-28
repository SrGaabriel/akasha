use thiserror::Error;

pub type DbResult<T> = Result<T, DbInternalError>;

#[derive(Debug, Error)]
pub enum DbInternalError {
    #[error("I/O error: {0}")]
    IoError(std::io::Error),
    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),
}

impl From<std::io::Error> for DbInternalError {
    fn from(err: std::io::Error) -> Self {
        DbInternalError::IoError(err)
    }
}