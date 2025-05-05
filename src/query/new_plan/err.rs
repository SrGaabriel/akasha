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
    #[error("Invalid argument list")]
    InvalidArgumentList,
    #[error("Invalid number of lambda parameters")]
    InvalidLambdaParams,
    #[error("Expected a lambda expression")]
    ExpectedLambda,
    #[error("Invalid argument for built-in function")]
    InvalidArgument,
    #[error("Too many arguments for function")]
    TooManyArguments,
    #[error("Unknown function name")]
    UnknownFunction,
}