use crate::frontend::err::QueryParsingError;
use std::fmt::Display;

pub type LexResult<'src> = Result<Vec<Token<'src>>, QueryParsingError>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Token<'src> {
    pub kind: TokenKind,
    pub value: &'src str,
    pub indent: usize,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    Number,
    Identifier,
    String,
    Plus,
    Minus,
    Asterisk,
    Slash,
    Percent,
    Application,
    LeftBraces,
    RightBraces,
    LeftParenthesis,
    RightParenthesis,
    Not,
    LeftBracket,
    RightBracket,
    GreaterThan,
    GreaterThanEquals,
    LessThan,
    LessThanEquals,
    And,
    Or,
    Comma,
    Dot,
    RightArrow,
    Equals,
    Lambda,
    Let,
    In,
    Do,
    Dollar,
    Newline,
    True,
    False,
    Pipe,
    EqualsEquals,
    NotEquals,
}

pub struct Lexer<'src> {
    source: &'src str,
    chars: std::str::CharIndices<'src>,
    current_char: Option<(usize, char)>,
    position: usize,
    line_start: bool,
    current_indent: usize,
}

impl<'src> Lexer<'src> {
    pub fn new(source: &'src str) -> Self {
        let mut chars = source.char_indices();
        let current_char = chars.next();
        Self {
            source,
            chars,
            current_char,
            position: 0,
            line_start: true,
            current_indent: 0,
        }
    }

    fn advance(&mut self) {
        self.current_char = self.chars.next();
        if let Some((pos, _)) = self.current_char {
            self.position = pos;
        }
    }

    fn peek(&self) -> Option<char> {
        self.current_char.map(|(_, c)| c)
    }

    fn peek_pos(&self) -> usize {
        self.current_char.map_or(self.source.len(), |(pos, _)| pos)
    }

    fn peek_next(&self) -> Option<char> {
        self.chars.clone().next().map(|(_, c)| c)
    }

    fn skip_whitespace(&mut self) -> usize {
        let mut count = 0;
        while let Some((_, c)) = self.current_char {
            if c == ' ' || c == '\t' {
                count += 1;
                self.advance();
            } else {
                break;
            }
        }
        count
    }

    fn skip_comment(&mut self) {
        while let Some((_, c)) = self.current_char {
            if c == '\n' {
                break;
            }
            self.advance();
        }
    }

    fn is_identifier_start(c: char) -> bool {
        c.is_alphabetic() || c == '_'
    }

    fn is_identifier_continue(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }

    fn is_digit(c: char) -> bool {
        c.is_ascii_digit()
    }

    fn read_identifier(&mut self) -> Token<'src> {
        let start_pos = self.peek_pos();
        self.advance();
        while let Some((_, c)) = self.current_char {
            if Self::is_identifier_continue(c) {
                self.advance();
            } else {
                break;
            }
        }
        let end_pos = self.peek_pos();
        let text = &self.source[start_pos..end_pos];
        let kind = match text {
            "let" => TokenKind::Let,
            "in" => TokenKind::In,
            "do" => TokenKind::Do,
            "true" => TokenKind::True,
            "false" => TokenKind::False,
            _ => TokenKind::Identifier,
        };
        Token {
            kind,
            value: text,
            indent: self.current_indent,
            span: Span {
                start: start_pos,
                end: end_pos,
            },
        }
    }

    fn read_number(&mut self) -> Token<'src> {
        let start_pos = self.peek_pos();
        if self.peek() == Some('-') {
            self.advance();
        }
        while let Some((_, c)) = self.current_char {
            if Self::is_digit(c) {
                self.advance();
            } else {
                break;
            }
        }
        if self.peek() == Some('.') {
            self.advance();
            if let Some((_, c)) = self.current_char {
                if Self::is_digit(c) {
                    self.advance();
                    while let Some((_, c)) = self.current_char {
                        if Self::is_digit(c) {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        let end_pos = self.peek_pos();
        Token {
            kind: TokenKind::Number,
            value: &self.source[start_pos..end_pos],
            indent: self.current_indent,
            span: Span {
                start: start_pos,
                end: end_pos,
            },
        }
    }

    fn read_string(&mut self) -> Result<Token<'src>, QueryParsingError> {
        let start_pos = self.peek_pos();
        self.advance();
        while let Some((_, c)) = self.current_char {
            if c == '"' {
                let end_pos = self.peek_pos() + 1;
                self.advance();
                return Ok(Token {
                    kind: TokenKind::String,
                    value: &self.source[start_pos..end_pos],
                    indent: self.current_indent,
                    span: Span {
                        start: start_pos,
                        end: end_pos,
                    },
                });
            }
            self.advance();
        }
        Err(QueryParsingError::UnterminatedString(start_pos))
    }

    fn read_arrow(&mut self) -> Token<'src> {
        let start_pos = self.peek_pos();
        self.advance(); // Consume '-'
        self.advance(); // Consume '>'
        let end_pos = self.peek_pos();
        Token {
            kind: TokenKind::RightArrow,
            value: &self.source[start_pos..end_pos],
            indent: self.current_indent,
            span: Span {
                start: start_pos,
                end: end_pos,
            },
        }
    }

    fn read_application(&mut self) -> Token<'src> {
        let start_pos = self.peek_pos();
        self.advance(); // Consume '|'
        self.advance(); // Consume '>'
        let end_pos = self.peek_pos();
        Token {
            kind: TokenKind::Application,
            value: &self.source[start_pos..end_pos],
            indent: self.current_indent,
            span: Span {
                start: start_pos,
                end: end_pos,
            },
        }
    }

    fn read_single_char_token(&mut self, c: char) -> Result<Token<'src>, QueryParsingError> {
        let start_pos = self.peek_pos();
        self.advance();
        let end_pos = self.peek_pos();
        let kind = match c {
            '+' => TokenKind::Plus,
            '-' => TokenKind::Minus,
            '*' => TokenKind::Asterisk,
            '/' => TokenKind::Slash,
            '{' => TokenKind::LeftBraces,
            '}' => TokenKind::RightBraces,
            '(' => TokenKind::LeftParenthesis,
            ')' => TokenKind::RightParenthesis,
            '[' => TokenKind::LeftBracket,
            ']' => TokenKind::RightBracket,
            ',' => TokenKind::Comma,
            '.' => TokenKind::Dot,
            '$' => TokenKind::Dollar,
            '|' => TokenKind::Pipe,
            '\\' => TokenKind::Lambda,
            '=' => TokenKind::Equals,
            '<' => TokenKind::LessThan,
            '>' => TokenKind::GreaterThan,
            '\n' => TokenKind::Newline,
            '\r' => {
                if self.peek() == Some('\n') {
                    self.advance();
                    let end_pos = self.peek_pos();
                    return Ok(Token {
                        kind: TokenKind::Newline,
                        value: &self.source[start_pos..end_pos],
                        indent: self.current_indent,
                        span: Span {
                            start: start_pos,
                            end: end_pos,
                        },
                    });
                }
                TokenKind::Newline
            }
            _ => return Err(QueryParsingError::UnexpectedCharacter(c)),
        };
        Ok(Token {
            kind,
            value: &self.source[start_pos..end_pos],
            indent: self.current_indent,
            span: Span {
                start: start_pos,
                end: end_pos,
            },
        })
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token<'src>>, QueryParsingError> {
        let mut tokens = Vec::new();
        while let Some((_, c)) = self.current_char {
            if self.line_start {
                self.current_indent = self.skip_whitespace();
                self.line_start = false;
                continue;
            }
            match c {
                ' ' => {
                    self.advance();
                    continue;
                }
                '\n' | '\r' => {
                    let token = self.read_single_char_token(c)?;
                    if token.kind == TokenKind::Newline {
                        self.line_start = true;
                    }
                    tokens.push(token);
                }
                c if Self::is_identifier_start(c) => {
                    tokens.push(self.read_identifier());
                }
                c if Self::is_digit(c) => {
                    tokens.push(self.read_number());
                }
                '"' => {
                    tokens.push(self.read_string()?);
                }
                '-' => {
                    if let Some(next_c) = self.peek_next() {
                        if next_c == '>' {
                            tokens.push(self.read_arrow());
                        } else if next_c == '-' {
                            self.skip_comment();
                            continue;
                        } else if next_c.is_digit(10) {
                            tokens.push(self.read_number());
                        } else {
                            tokens.push(self.read_single_char_token(c)?);
                        }
                    } else {
                        tokens.push(self.read_single_char_token(c)?);
                    }
                }
                '|' => {
                    if let Some(next_c) = self.peek_next() {
                        if next_c == '>' {
                            tokens.push(self.read_application());
                        } else {
                            tokens.push(self.read_single_char_token(c)?);
                        }
                    } else {
                        tokens.push(self.read_single_char_token(c)?);
                    }
                }
                '+' | '*' | '/' | '(' | ')' | '{' | '}' | '[' | ']' | ',' | '.' | '$' | '='
                | '\\' | '<' | '>' => {
                    tokens.push(self.read_single_char_token(c)?);
                }
                _ => return Err(QueryParsingError::UnexpectedCharacter(c)),
            }
        }
        Ok(tokens)
    }
}

impl Display for TokenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ref_name = match self {
            TokenKind::Number => "Number",
            TokenKind::Identifier => "Identifier",
            TokenKind::String => "String",
            TokenKind::Plus => "Plus",
            TokenKind::Minus => "Minus",
            TokenKind::Asterisk => "Asterisk",
            TokenKind::Slash => "Slash",
            TokenKind::Application => "Application",
            TokenKind::LeftParenthesis => "LeftParenthesis",
            TokenKind::RightParenthesis => "RightParenthesis",
            TokenKind::LeftBracket => "LeftBracket",
            TokenKind::RightBracket => "RightBracket",
            TokenKind::LeftBraces => "LeftBraces",
            TokenKind::RightBraces => "RightBraces",
            TokenKind::GreaterThan => "GreaterThan",
            TokenKind::LessThan => "LessThan",
            TokenKind::Comma => "Comma",
            TokenKind::Dot => "Dot",
            TokenKind::RightArrow => "RightArrow",
            TokenKind::Equals => "Equals",
            TokenKind::Lambda => "Lambda",
            TokenKind::Let => "Let",
            TokenKind::In => "In",
            TokenKind::Do => "Do",
            TokenKind::Dollar => "Dollar",
            TokenKind::Newline => "Newline",
            TokenKind::True => "True",
            TokenKind::False => "False",
            TokenKind::Pipe => "Pipe",
            TokenKind::EqualsEquals => "EqualsEquals",
            TokenKind::NotEquals => "NotEquals",
            TokenKind::And => "And",
            TokenKind::Or => "Or",
            TokenKind::Not => "Not",
            TokenKind::GreaterThanEquals => "GreaterThanEquals",
            TokenKind::LessThanEquals => "LessThanEquals",
            TokenKind::Percent => "Percent",
        };
        write!(f, "{}", ref_name)
    }
}
