pub type QueryResult<T> = Result<T, QueryError>;

#[derive(Debug)]
pub enum QueryError {
    TableNotFound(String),
    InvalidSchema(String),
    InvalidTuple(String),
    InvalidFilter(String),
    InvalidOperation(String),
    IoError(std::io::Error),
    ColumnNotFound(String),
    ValueAndDefaultMissing(String),
}