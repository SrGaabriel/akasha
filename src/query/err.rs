use crate::frontend::ast::Expr;
use crate::frontend::lexer::TokenKind;
use thiserror::Error;

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
    },
}

pub type QueryResult<T> = Result<T, QueryError>;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Table '{0}' not found")]
    TableNotFound(String),
    #[error("Column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),
    #[error("The query is not a transaction")]
    NotATransaction,
    #[error("Unknown reference '{0}'")]
    SymbolNotFound(String),
    #[error("Only rows can be inserted into a table")]
    ExpectedRow,
    #[error("Expected a value, but found a row")]
    RowCannotBeEmbeddedIntoAnotherRow,
}
