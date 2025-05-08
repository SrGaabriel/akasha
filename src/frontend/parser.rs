use crate::frontend::ast::{Arena, NodeId};
use crate::frontend::lexer::{Token, TokenKind};

#[derive(Debug, Clone)]
pub enum ParseError<'src> {
    NotAnExpression(Token<'src>),
    ExpectedAnExpression(Token<'src>),
    ExpectedToken(TokenKind, Token<'src>),
    ExpectedDifferentIndentation {
        token: Token<'src>,
        expected: usize,
        actual: usize,
    },
    UnexpectedEndOfInput,
    Custom(String),
}

pub struct Parser<'src> {
    tokens: &'src [Token<'src>],
    pos: usize,
    arena: &'src mut Arena,
}

impl<'src> Parser<'src> {
    pub fn new(tokens: &'src [Token<'src>], arena: &'src mut Arena) -> Self {
        Self { tokens, pos: 0, arena }
    }

    pub fn parse_expression(&mut self) -> Result<NodeId, ParseError<'src>> {
        self.expression()
    }

    fn peek(&self) -> Result<Token<'src>, ParseError<'src>> {
        self.tokens.get(self.pos).copied().ok_or(ParseError::UnexpectedEndOfInput)
    }

    fn peek_ahead(&self, n: usize) -> Result<Token<'src>, ParseError<'src>> {
        self.tokens.get(self.pos + n).copied().ok_or(ParseError::UnexpectedEndOfInput)
    }

    fn consume(&mut self) -> Result<Token<'src>, ParseError<'src>> {
        let token = self.peek()?;
        self.pos += 1;
        Ok(token)
    }

    fn expect(&mut self, kind: TokenKind) -> Result<Token<'src>, ParseError<'src>> {
        let token = self.peek()?;
        if token.kind == kind {
            self.pos += 1;
            Ok(token)
        } else {
            Err(ParseError::ExpectedToken(kind, token))
        }
    }

    fn expect_relevant(&mut self, kind: TokenKind) -> Result<Token<'src>, ParseError<'src>> {
        let token = self.peek_relevant()?;
        if token.kind == kind {
            self.skip_newlines();
            self.pos += 1;
            Ok(token)
        } else {
            Err(ParseError::ExpectedToken(kind, token))
        }
    }

    fn peek_is_any(&self, kinds: &[TokenKind]) -> bool {
        if let Ok(token) = self.peek() {
            kinds.contains(&token.kind)
        } else {
            false
        }
    }

    fn peek_is_any_relevant(&self, kinds: &[TokenKind]) -> bool {
        if let Ok(token) = self.peek_relevant() {
            kinds.contains(&token.kind)
        } else {
            false
        }
    }

    fn skip_newlines(&mut self) {
        while let Ok(token) = self.peek() {
            if token.kind == TokenKind::Newline {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn peek_relevant(&self) -> Result<Token<'src>, ParseError<'src>> {
        let mut pos = self.pos;
        loop {
            match self.tokens.get(pos) {
                Some(token) if token.kind == TokenKind::Newline => pos += 1,
                Some(token) => return Ok(*token),
                None => return Err(ParseError::UnexpectedEndOfInput),
            }
        }
    }

    fn consume_relevant(&mut self) -> Result<Token<'src>, ParseError<'src>> {
        self.skip_newlines();
        self.consume()
    }

    fn save_position(&self) -> Result<usize, ParseError<'src>> {
        self.peek()?;
        Ok(self.pos)
    }

    fn restore_position(&mut self, pos: usize) {
        self.pos = pos;
    }

    fn expression(&mut self) -> Result<NodeId, ParseError<'src>> {
        self.pipe_expression()
    }

    fn pipe_expression(&mut self) -> Result<NodeId, ParseError<'src>> {
        let mut left = self.comparison_expression()?;

        while self.peek_is_any(&[TokenKind::Application]) {
            self.consume()?;
            let right = self.comparison_expression()?;

            if let Some((func, mut args)) = self.arena.extract_function_call(right) {
                args.push(left);
                left = self.arena.create_function_call(func, &args);
            } else {
                left = self.arena.create_function_call(right, &[left]);
            }
        }
        Ok(left)
    }

    fn numeric_expression(&mut self) -> Result<NodeId, ParseError<'src>> {
        let mut left = self.term()?;

        while self.peek_is_any(&[TokenKind::Plus, TokenKind::Minus]) {
            let op_token = self.consume()?;
            let right = self.term()?;
            left = self.arena.create_binary_op(op_token.kind, left, right);
        }

        Ok(left)
    }

    fn term(&mut self) -> Result<NodeId, ParseError<'src>> {
        let mut left = self.application()?;

        while self.peek_is_any(&[TokenKind::Asterisk, TokenKind::Slash]) {
            let op_token = self.consume()?;
            let right = self.application()?;
            left = self.arena.create_binary_op(op_token.kind, left, right);
        }

        Ok(left)
    }

    fn application(&mut self) -> Result<NodeId, ParseError<'src>> {
        let first = self.field_access()?;

        let mut items = vec![first];
        while let Ok(current_pos) = self.save_position() {
            if self.peek_is_any(&[
                TokenKind::Plus, TokenKind::Minus, TokenKind::Asterisk, TokenKind::Slash,
                TokenKind::RightParenthesis, TokenKind::RightBracket, TokenKind::Comma,
                TokenKind::In, TokenKind::Newline, TokenKind::Application
            ]) {
                break;
            }

            match self.field_access() {
                Ok(item) => items.push(item),
                Err(err) => {
                    println!("Debug application end error: {err:?}");
                    self.restore_position(current_pos);
                    break;
                }
            }
        }

        if items.len() == 1 {
            return Ok(items[0]);
        }

        let result = self.arena.create_function_call(items[0], &items[1..]);
        Ok(result)
    }

    fn atom(&mut self) -> Result<NodeId, ParseError<'src>> {
        let token = self.peek()?;

        match token.kind {
            TokenKind::Number => {
                self.consume()?;
                Ok(self.arena.create_number(token.value))
            }
            TokenKind::Identifier => {
                self.consume()?;
                Ok(self.arena.create_reference(token.value))
            }
            TokenKind::String => {
                self.consume()?;
                Ok(self.arena.create_string_lit(token.value))
            }
            TokenKind::True => {
                self.consume()?;
                Ok(self.arena.create_bool(true))
            }
            TokenKind::False => {
                self.consume()?;
                Ok(self.arena.create_bool(false))
            }
            TokenKind::LeftBraces => self.instance_expr(),
            TokenKind::LeftParenthesis => self.paren_expr(),
            TokenKind::LeftBracket => self.array_expr(),
            TokenKind::Let => self.let_expr(),
            TokenKind::Dollar => {
                self.consume()?;
                self.expression()
            }
            TokenKind::Do => self.do_expr(),
            TokenKind::Lambda => self.lambda_short_form(),
            _ => Err(ParseError::NotAnExpression(token)),
        }
    }

    fn field_access(&mut self) -> Result<NodeId, ParseError<'src>> {
        let mut expr = self.atom()?;

        while self.peek_is_any(&[TokenKind::Dot]) {
            self.consume()?;
            let field_token = self.expect(TokenKind::Identifier)?;
            expr = self.arena.create_field_access(expr, field_token.value);
        }

        Ok(expr)
    }

    fn lambda_short_form(&mut self) -> Result<NodeId, ParseError<'src>> {
        self.expect(TokenKind::Lambda)?;

        let mut param_names = Vec::new();

        loop {
            if self.peek_is_any(&[TokenKind::RightArrow]) {
                break;
            }

            let id_token = self.expect(TokenKind::Identifier)?;
            param_names.push(id_token.value);

            if self.peek_is_any(&[TokenKind::RightArrow]) {
                break;
            }
        }

        if param_names.is_empty() {
            return Err(ParseError::Custom("Lambda expression needs at least one parameter".to_string()));
        }

        self.expect(TokenKind::RightArrow)?;
        let body = self.expression()?;

        Ok(self.arena.create_lambda(&*param_names, body))
    }

    fn paren_expr(&mut self) -> Result<NodeId, ParseError<'src>> {
        self.expect(TokenKind::LeftParenthesis)?;

        if self.peek_is_any(&[TokenKind::Lambda]) {
            let result = self.lambda_expr()?;
            self.expect(TokenKind::RightParenthesis)?;
            Ok(result)
        } else {
            let items = self.comma_separated_expressions(TokenKind::RightParenthesis)?;
            if items.len() == 1 {
                Ok(items[0])
            } else {
                Ok(self.arena.create_tuple(&items))
            }
        }
    }

    fn array_expr(&mut self) -> Result<NodeId, ParseError<'src>> {
        self.expect(TokenKind::LeftBracket)?;
        let items = self.comma_separated_expressions(TokenKind::RightBracket)?;
        Ok(self.arena.create_array(&items))
    }

    // it goes like this: { name = "value", arg2 = 5 }, etc..
    fn instance_expr(&mut self) -> Result<NodeId, ParseError<'src>> {
        self.expect(TokenKind::LeftBraces)?;
        let mut items = Vec::new();

        loop {
            if self.peek_is_any_relevant(&[TokenKind::RightBraces]) {
                break;
            }

            let id_token = self.expect_relevant(TokenKind::Identifier)?;
            self.expect(TokenKind::Equals)?;
            let value = self.expression()?;
            items.push((id_token.value, value));

            if !self.peek_is_any(&[TokenKind::Comma]) {
                break;
            }
            self.consume()?;
        }
        self.expect_relevant(TokenKind::RightBraces)?;
        Ok(self.arena.create_instance(&items))
    }

    fn lambda_expr(&mut self) -> Result<NodeId, ParseError<'src>> {
        self.expect(TokenKind::Lambda)?;

        let mut param_names = Vec::new();

        let id_token = self.expect(TokenKind::Identifier)?;
        param_names.push(id_token.value);

        loop {
            if self.peek_is_any(&[TokenKind::RightArrow]) {
                break;
            }

            self.expect(TokenKind::Dot)?;
            let id_token = self.expect(TokenKind::Identifier)?;
            param_names.push(id_token.value);
        }

        self.expect(TokenKind::RightArrow)?;
        let body = self.expression()?;

        Ok(self.arena.create_lambda(&*param_names, body))
    }

    fn let_expr(&mut self) -> Result<NodeId, ParseError<'src>> {
        self.expect(TokenKind::Let)?;

        let start_pos = self.save_position()?;
        if let Ok(token) = self.peek() {
            if token.kind == TokenKind::Newline {
                self.consume()?;
                let let_indent = token.indent;
                let bindings = self.parse_indented_bindings(let_indent)?;
                self.skip_newlines();
                self.expect(TokenKind::In)?;
                let body = self.expression()?;

                return self.create_nested_lets(bindings, body);
            }
        }

        self.restore_position(start_pos);
        let id_token = self.expect(TokenKind::Identifier)?;
        self.expect(TokenKind::Equals)?;
        let value = self.expression()?;
        self.skip_newlines();
        self.expect(TokenKind::In)?;
        let body = self.expression()?;

        Ok(self.arena.create_let(id_token.value, value, body))
    }

    fn parse_indented_bindings(&mut self, indent: usize) -> Result<Vec<(&'src str, NodeId)>, ParseError<'src>> {
        let mut bindings = Vec::new();

        loop {
            if let Ok(token) = self.peek_relevant() {
                if token.indent < indent || token.kind == TokenKind::In {
                    break;
                }

                let id_token = self.expect(TokenKind::Identifier)?;
                self.expect(TokenKind::Equals)?;
                let value = self.expression()?;

                bindings.push((id_token.value, value));

                self.skip_newlines();
            } else {
                break;
            }
        }

        if bindings.is_empty() {
            return Err(ParseError::Custom("Expected at least one binding in let expression".to_string()));
        }

        Ok(bindings)
    }

    fn comparison_expression(&mut self) -> Result<NodeId, ParseError<'src>> {
        let mut left = self.numeric_expression()?;

        while self.peek_is_any(&[
            TokenKind::GreaterThan,
            TokenKind::LessThan,
            TokenKind::EqualsEquals,
            TokenKind::NotEquals
        ]) {
            let op_token = self.consume()?;
            let right = self.numeric_expression()?;
            left = self.arena.create_binary_op(op_token.kind, left, right);
        }

        Ok(left)
    }

    fn create_nested_lets(&mut self, bindings: Vec<(&'src str, NodeId)>, body: NodeId) -> Result<NodeId, ParseError<'src>> {
        let mut result = body;

        for (name, value) in bindings.into_iter().rev() {
            result = self.arena.create_let(name, value, result);
        }

        Ok(result)
    }

    fn do_expr(&mut self) -> Result<NodeId, ParseError<'src>> {
        let do_token = self.expect(TokenKind::Do)?;
        let indent = do_token.indent;

        let exprs = self.indented_block(indent)?;
        Ok(self.arena.create_block(&exprs))
    }

    fn comma_separated_expressions(&mut self, end_token: TokenKind) -> Result<Vec<NodeId>, ParseError<'src>> {
        if let Ok(token) = self.peek() {
            if token.kind == end_token {
                self.consume()?;
                return Ok(Vec::new());
            }
        }

        let mut exprs = Vec::new();

        exprs.push(self.expression()?);

        loop {
            if let Ok(token) = self.peek() {
                if token.kind == end_token {
                    self.consume()?;
                    break;
                } else if token.kind == TokenKind::Comma {
                    self.consume()?;
                    if let Ok(next) = self.peek() {
                        if next.kind == end_token {
                            self.consume()?;
                            break;
                        }
                    }
                    exprs.push(self.expression()?);
                } else {
                    return Err(ParseError::ExpectedToken(TokenKind::Comma, token));
                }
            } else {
                return Err(ParseError::UnexpectedEndOfInput);
            }
        }

        Ok(exprs)
    }

    fn indented_block(&mut self, indent: usize) -> Result<Vec<NodeId>, ParseError<'src>> {
        let mut exprs = Vec::new();

        exprs.push(self.expression()?);

        loop {
            let current_pos = self.save_position()?;

            if let Ok(token) = self.peek() {
                if token.kind == TokenKind::Newline {
                    self.consume()?;

                    if let Ok(token) = self.peek() {
                        if token.indent < indent {
                            self.restore_position(current_pos);
                            break;
                        }

                        exprs.push(self.expression()?);
                        continue;
                    }
                }
            }

            self.restore_position(current_pos);
            break;
        }

        Ok(exprs)
    }
}

pub fn parse_expression<'src>(tokens: &'src [Token<'src>], arena: &'src mut Arena) -> Result<NodeId, ParseError<'src>> {
    let mut parser = Parser::new(tokens, arena);
    parser.parse_expression()
}