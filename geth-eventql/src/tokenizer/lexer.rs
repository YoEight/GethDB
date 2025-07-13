use crate::{
    sym::{Keyword, Literal, Operation, Sym},
    tokenizer::{Pos, text::Text},
};

pub struct Lexer<'a> {
    text: Text<'a>,
}

impl<'a> Lexer<'a> {
    fn consume_and_return(&mut self, sym: Sym) -> eyre::Result<Option<Sym>> {
        self.text.shift();
        Ok(Some(sym))
    }

    pub fn pos(&self) -> Pos {
        self.text.pos()
    }

    pub fn next_sym(&mut self) -> eyre::Result<Option<Sym>> {
        match self.text.look_ahead() {
            None => Ok(None),

            Some(c) => match c {
                _ if c.is_ascii_whitespace() => {
                    self.text.shift();

                    while let Some(c) = self.text.look_ahead() {
                        if !c.is_ascii_whitespace() {
                            break;
                        }

                        self.text.shift();
                    }

                    Ok(Some(Sym::Whitespace))
                }

                '.' => self.consume_and_return(Sym::Dot),
                ',' => self.consume_and_return(Sym::Comma),
                ':' => self.consume_and_return(Sym::Colon),
                '[' => self.consume_and_return(Sym::LBracket),
                ']' => self.consume_and_return(Sym::RBracket),
                '(' => self.consume_and_return(Sym::LBrace),
                ')' => self.consume_and_return(Sym::RBrace),

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
                        eyre::bail!("{}: unexpected end of the query", self.text.pos());
                    };

                    if c == '=' {
                        return self.consume_and_return(Sym::Operation(Operation::NotEqual));
                    }

                    eyre::bail!("{}: unexpected symbol '{c}'", self.text.pos())
                }

                '=' => {
                    self.text.shift();

                    let c = if let Some(c) = self.text.look_ahead() {
                        c
                    } else {
                        eyre::bail!("{}: unexpected end of the query", self.text.pos());
                    };

                    if c == '=' {
                        return self.consume_and_return(Sym::Operation(Operation::Equal));
                    }

                    eyre::bail!("{}: unexpected symbol '{c}'", self.text.pos())
                }

                _ if c.is_ascii_lowercase() || c.is_ascii_uppercase() => {
                    let mut ident = String::new();

                    ident.push(c);
                    self.text.shift();

                    while let Some(c) = self.text.look_ahead() {
                        if !c.is_ascii_alphanumeric() && c != '_' {
                            break;
                        }

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

                _ => eyre::bail!("{}: unexpected symbol '{c}'", self.text.pos()),
            },
        }
    }

    fn shift_or_bail(&mut self) -> eyre::Result<char> {
        if let Some(c) = self.text.shift() {
            return Ok(c);
        }

        eyre::bail!("unexpected end of query")
    }

    fn parse_integer_or_float(&mut self) -> eyre::Result<Option<Sym>> {
        let c = self.shift_or_bail()?;
        let mut num = String::new();

        num.push(c);
        self.text.shift();

        let mut is_float = false;
        while let Some(ch) = self.text.look_ahead() {
            if ch == '.' {
                if is_float {
                    eyre::bail!("{}: malformed floating number", self.text.pos());
                }

                is_float = true;
            } else if !ch.is_ascii_digit() {
                break;
            }

            num.push(ch);
            self.text.shift();
        }

        if is_float {
            match num.parse::<f64>() {
                Ok(num) => return Ok(Some(Sym::Literal(Literal::Float(num)))),
                Err(e) => {
                    eyre::bail!("{}: malformed floating number: {e}", self.text.pos())
                }
            }
        }

        match num.parse::<i64>() {
            Ok(num) => Ok(Some(Sym::Literal(Literal::Integral(num)))),
            Err(e) => {
                eyre::bail!("{}: malformed integral number: {e}", self.text.pos())
            }
        }
    }

    fn parse_string_literal(&mut self) -> eyre::Result<Option<Sym>> {
        let opening = self.shift_or_bail()?;
        let mut string = String::new();

        self.text.shift();

        while let Some(ch) = self.text.look_ahead() {
            if ch == opening {
                return self.consume_and_return(Sym::Literal(Literal::String(string)));
            }

            if ch == '\n' {
                eyre::bail!("{}: string literal is malformed", self.text.pos());
            }

            string.push(ch);
            self.text.shift();
        }

        eyre::bail!("incomplete string literal");
    }
}
