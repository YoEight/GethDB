use crate::{
    error::LexerError,
    sym::{Keyword, Literal, Operation, Sym},
    tokenizer::{Pos, text::Text},
};

pub struct Lexer<'a> {
    text: Text<'a>,
}

impl<'a> From<Text<'a>> for Lexer<'a> {
    fn from(text: Text<'a>) -> Lexer<'a> {
        Lexer { text }
    }
}

impl<'a> Lexer<'a> {
    pub fn new(query: &'a str) -> Self {
        Text::new(query).into()
    }

    fn consume_and_return(&mut self, sym: Sym) -> crate::Result<Option<Sym>> {
        self.text.shift();
        Ok(Some(sym))
    }

    pub fn pos(&self) -> Pos {
        self.text.pos()
    }

    pub fn next_sym(&mut self) -> crate::Result<Option<Sym>> {
        match self.text.look_ahead() {
            None => Ok(None),

            Some(c) => match c {
                _ if c.is_ascii_whitespace() => {
                    self.text.shift();

                    while let Some(c) = self.text.look_ahead()
                        && c.is_ascii_whitespace()
                    {
                        self.text.shift();
                    }

                    Ok(Some(Sym::Whitespace))
                }

                '.' => self.consume_and_return(Sym::Dot),
                ',' => self.consume_and_return(Sym::Comma),
                ':' => self.consume_and_return(Sym::Colon),
                '[' => self.consume_and_return(Sym::LBracket),
                ']' => self.consume_and_return(Sym::RBracket),
                '{' => self.consume_and_return(Sym::LBrace),
                '}' => self.consume_and_return(Sym::RBrace),
                ')' => self.consume_and_return(Sym::RParens),
                '(' => self.consume_and_return(Sym::LParens),

                '<' => {
                    self.text.shift();

                    if let Some('=') = self.text.look_ahead() {
                        return self.consume_and_return(Sym::Operation(Operation::LessThanOrEqual));
                    }

                    self.consume_and_return(Sym::Operation(Operation::LessThan))
                }

                '>' => {
                    self.text.shift();

                    if let Some('=') = self.text.look_ahead() {
                        return self
                            .consume_and_return(Sym::Operation(Operation::GreaterThanOrEqual));
                    }

                    self.consume_and_return(Sym::Operation(Operation::GreaterThan))
                }

                '!' => {
                    self.text.shift();

                    let c = if let Some(c) = self.text.look_ahead() {
                        c
                    } else {
                        bail!(self.text.pos(), LexerError::UnexpectedEndOfQuery);
                    };

                    if c == '=' {
                        return self.consume_and_return(Sym::Operation(Operation::NotEqual));
                    }

                    bail!(self.text.pos(), LexerError::UnexpectedSymbol(c));
                }

                '=' => {
                    self.text.shift();

                    let c = if let Some(c) = self.text.look_ahead() {
                        c
                    } else {
                        bail!(self.text.pos(), LexerError::UnexpectedEndOfQuery);
                    };

                    if c == '=' {
                        return self.consume_and_return(Sym::Operation(Operation::Equal));
                    }

                    bail!(self.text.pos(), LexerError::UnexpectedSymbol(c));
                }

                _ if c.is_ascii_lowercase() || c.is_ascii_uppercase() => {
                    let mut ident = String::new();

                    ident.push(c);
                    self.text.shift();

                    while let Some(c) = self.text.look_ahead()
                        && (c.is_ascii_alphanumeric() || c == '_')
                    {
                        ident.push(c);
                        self.text.shift();
                    }

                    match ident.to_lowercase().as_str() {
                        "true" => Ok(Some(Sym::Literal(Literal::Bool(true)))),
                        "false" => Ok(Some(Sym::Literal(Literal::Bool(false)))),
                        "from" => Ok(Some(Sym::Keyword(Keyword::From))),
                        "in" => Ok(Some(Sym::Keyword(Keyword::In))),
                        "where" => Ok(Some(Sym::Keyword(Keyword::Where))),
                        "order" => Ok(Some(Sym::Keyword(Keyword::Order))),
                        "by" => Ok(Some(Sym::Keyword(Keyword::By))),
                        "asc" => Ok(Some(Sym::Keyword(Keyword::Asc))),
                        "desc" => Ok(Some(Sym::Keyword(Keyword::Desc))),
                        "group" => Ok(Some(Sym::Keyword(Keyword::Group))),
                        "skip" => Ok(Some(Sym::Keyword(Keyword::Skip))),
                        "top" => Ok(Some(Sym::Keyword(Keyword::Top))),
                        "project" => Ok(Some(Sym::Keyword(Keyword::Project))),
                        "into" => Ok(Some(Sym::Keyword(Keyword::Into))),
                        "distinct" => Ok(Some(Sym::Keyword(Keyword::Distinct))),
                        "having" => Ok(Some(Sym::Keyword(Keyword::Having))),
                        "as" => Ok(Some(Sym::Keyword(Keyword::As))),
                        "if" => Ok(Some(Sym::Keyword(Keyword::If))),
                        "contains" => Ok(Some(Sym::Operation(Operation::Contains))),
                        "and" => Ok(Some(Sym::Operation(Operation::And))),
                        "or" => Ok(Some(Sym::Operation(Operation::Or))),
                        "xor" => Ok(Some(Sym::Operation(Operation::Xor))),
                        "not" => Ok(Some(Sym::Operation(Operation::Not))),
                        _ => Ok(Some(Sym::Id(ident))),
                    }
                }

                _ if c.is_ascii_digit() || c == '-' => self.parse_integer_or_float(),

                '"' | '\'' => self.parse_string_literal(),

                _ => bail!(self.text.pos(), LexerError::UnexpectedSymbol(c)),
            },
        }
    }

    fn shift_or_bail(&mut self) -> crate::Result<char> {
        if let Some(c) = self.text.shift() {
            return Ok(c);
        }

        bail!(self.text.pos(), LexerError::UnexpectedEndOfQuery);
    }

    fn parse_integer_or_float(&mut self) -> crate::Result<Option<Sym>> {
        let c = self.shift_or_bail()?;
        let mut num = String::new();
        num.push(c);

        let mut is_float = false;
        while let Some(ch) = self.text.look_ahead() {
            if ch == '.' {
                if is_float {
                    bail!(self.text.pos(), LexerError::MalformedFloatingNumber(None));
                }

                is_float = true;
            } else if !ch.is_ascii_digit() && ch != '-' {
                break;
            }

            num.push(ch);
            self.text.shift();
        }

        if is_float {
            match num.parse::<f64>() {
                Ok(num) => return Ok(Some(Sym::Literal(Literal::Float(num)))),
                Err(e) => {
                    bail!(
                        self.text.pos(),
                        LexerError::MalformedFloatingNumber(Some(e))
                    );
                }
            }
        }

        match num.parse::<i64>() {
            Ok(num) => Ok(Some(Sym::Literal(Literal::Integral(num)))),
            Err(e) => {
                bail!(self.text.pos(), LexerError::MalformedIntegralNumber(e))
            }
        }
    }

    fn parse_string_literal(&mut self) -> crate::Result<Option<Sym>> {
        let opening = self.shift_or_bail()?;
        let mut string = String::new();

        while let Some(ch) = self.text.look_ahead() {
            if ch == opening {
                return self.consume_and_return(Sym::Literal(Literal::String(string)));
            }

            if ch == '\n' {
                bail!(self.text.pos(), LexerError::StringLiteralNotClosed);
            }

            string.push(ch);
            self.text.shift();
        }

        bail!(self.text.pos(), LexerError::UnexpectedEndOfQuery);
    }
}
