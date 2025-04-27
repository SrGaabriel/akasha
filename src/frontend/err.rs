use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryParsingError {
    #[error("The character `{0}` is not supported")]
    UnexpectedCharacter(char),
    #[error("The string starting at position {0} is not terminated")]
    UnterminatedString(usize),
}

