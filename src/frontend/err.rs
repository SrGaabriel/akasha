use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryParsingError {
    #[error("The character `{0}` is not supported")]
    UnexpectedCharacter(char)
}

