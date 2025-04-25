// TODO: migrate to nom, combine or chomp

use crate::frontend::err::QueryParsingError;

pub type LexResult<'a> = Result<Vec<Token<'a>>, QueryParsingError>;

#[derive(Debug, Clone, PartialEq)]
pub enum Token<'a> {
    Let,
    In,
    Identifier(&'a str),
    Number(&'a str),
    Pipe,
    Arrow,
    Equals,
    Comma,
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    LeftCurly,
    RightCurly,
    Dot,
    Lambda,
    Minus,
    Plus,
    Asterisk,
    Slash,
    Percentage,
    LeftArrow,
    RightArrow,
    LeftAngleBracket,
    RightAngleBracket,
    StringLit(&'a str),
}

pub fn tokenize(input: &str) -> LexResult {
    let mut tokens = Vec::with_capacity(input.len() / 4);
    let bytes = input.as_bytes();
    let mut offset = 0;

    while offset < bytes.len() {
        let b = bytes[offset];

        if b.is_ascii_whitespace() {
            offset += 1;
            continue;
        }

        let remaining = &input[offset..];
        match b {
            b'(' => { tokens.push(Token::LeftParen); offset += 1; }
            b')' => { tokens.push(Token::RightParen); offset += 1; }
            b'{' => { tokens.push(Token::LeftCurly); offset += 1; }
            b'}' => { tokens.push(Token::RightCurly); offset += 1; }
            b'[' => { tokens.push(Token::LeftBracket); offset += 1; }
            b']' => { tokens.push(Token::RightBracket); offset += 1; }
            b'<' => { tokens.push(Token::LeftAngleBracket); offset += 1 }
            b'>' => { tokens.push(Token::RightAngleBracket); offset += 1 }
            b',' => { tokens.push(Token::Comma); offset += 1; }
            b'.' => { tokens.push(Token::Dot); offset += 1; }
            b'\\' => { tokens.push(Token::Lambda); offset += 1 }
            b'=' => {
                if bytes.get(offset + 1) == Some(&b'>') {
                    tokens.push(Token::Arrow);
                    offset += 2;
                } else {
                    tokens.push(Token::Equals);
                    offset += 1;
                }
            }
            b'|' => {
                if bytes.get(offset + 1) == Some(&b'>') {
                    tokens.push(Token::Pipe);
                    offset += 2;
                } else {
                    panic!("Unexpected `|` without `>`");
                }
            }
            b'"' => {
                let (s, len) = parse_string(remaining);
                tokens.push(Token::StringLit(s));
                offset += len;
            }
            b'-' => {
                match bytes.get(offset + 1) {
                    Some(&b'-') => {
                        let mut i = 2;
                        while offset+i < bytes.len() && (bytes[offset + i] != b'\n') {
                            i += 1;
                        }
                        offset += i;
                    }
                    Some(&b'>') => { tokens.push(Token::RightArrow); offset += 2; }
                    _ => { tokens.push(Token::Minus); offset += 1; }
                }
            }
            b'0'..=b'9' => {
                let (num, len) = parse_number(remaining);
                tokens.push(Token::Number(num));
                offset += len;
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                let (ident, len) = parse_identifier(remaining);
                tokens.push(match ident {
                    "let" => Token::Let,
                    "in" => Token::In,
                    _ => Token::Identifier(ident),
                });
                offset += len;
            }
            _ => return Err(QueryParsingError::UnexpectedCharacter(char::from(b)))
        }
    }
    Ok(tokens)
}

fn parse_number(input: &str) -> (&str, usize) {
    let bytes = input.as_bytes();
    let mut len = 0;
    while len < bytes.len() && bytes[len].is_ascii_digit() {
        len += 1;
    }
    (&input[..len], len)
}

fn parse_identifier(input: &str) -> (&str, usize) {
    let bytes = input.as_bytes();
    let mut len = 0;
    while len < bytes.len() && (
        bytes[len].is_ascii_alphanumeric() || bytes[len] == b'_'
    ) {
        len += 1;
    }
    (&input[..len], len)
}

fn parse_string(input: &str) -> (&str, usize) {
    let bytes = input.as_bytes();
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => break,
            b'\\' => i += 2,
            _ => i += 1,
        }
    }
    if i >= bytes.len() {
        panic!("Unterminated string literal");
    }
    (&input[1..i], i + 1)
}
