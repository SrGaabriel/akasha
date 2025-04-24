pub type QueryResult<T> = Result<T, QueryError>;

#[derive(Debug)] // todo: better errors
pub enum QueryError {
    TableNotFound(String),
    InvalidSchema(String),
    InvalidTuple(String),
    InvalidFilter(String),
    InvalidOperation(String),
    IoError(std::io::Error),
    ColumnNotFound(String),
    ValueAndDefaultMissing(String),
    NotImplemented(String),
    ValueAndColumnMismatch(usize, usize),
}