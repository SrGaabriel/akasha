use thiserror::Error;
use crate::frontend::ast::Expr;
use crate::frontend::lexer::TokenKind;

#[derive(Debug, Error)]
pub enum TransformError {
    #[error("Empty block")]
    EmptyBlock,
    #[error("Undefined reference: {0}")]
    UndefinedReference(String),
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(TokenKind),
    #[error("Invalid field access")]
    InvalidFieldAccess,
    #[error("Invalid number")]
    InvalidNumber,
    #[error("Unsupported expression: {0:?}")]
    UnsupportedExpression(Expr),
    #[error("Invalid number of lambda parameters")]
    InvalidLambdaParams,
    #[error("Expected a lambda expression")]
    ExpectedLambda,
    #[error("Invalid argument for built-in function {0}")]
    InvalidArgument(String),
    #[error("Too many arguments for function")]
    TooManyArguments,
    #[error("Unknown function name")]
    UnknownFunction,
    #[error("Expected {expected} arguments for function `{name}`, but found {found}")]
    WrongNumberOfArguments {
        name: String,
        expected: usize,
        found: usize,
    }
}

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
    ExpectedValue(String),
    NotATransaction,
    SymbolNotFound(String),
    ExpectedRow,
    ValueAndColumnMismatch(usize, usize),
    RowCannotBeEmbeddedIntoAnotherRow
}