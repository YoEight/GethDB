use crate::{
    sym::{Comparison, Keyword, Literal, Sym},
    tokenizer::text::Text,
};

pub struct Lexer<'a> {
    text: Text<'a>,
}

impl<'a> Lexer<'a> {
    fn consume_and_return(&mut self, sym: Sym) -> eyre::Result<Option<Sym>> {
        self.text.shift();
        Ok(Some(sym))
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
                        return self
                            .consume_and_return(Sym::Comparison(Comparison::LessThanOrEqual));
                    }

                    self.consume_and_return(Sym::Comparison(Comparison::LessThan))
                }

                '>' => {
                    self.text.shift();

                    if let Some('=') = self.text.look_ahead() {
                        return self
                            .consume_and_return(Sym::Comparison(Comparison::GreaterThanOrEqual));
                    }

                    self.consume_and_return(Sym::Comparison(Comparison::GreaterThan))
                }

                '!' => {
                    self.text.shift();

                    let c = if let Some(c) = self.text.look_ahead() {
                        c
                    } else {
                        eyre::bail!("{}: unexpected end of the query", self.text.pos());
                    };

                    if c == '=' {
                        return self.consume_and_return(Sym::Comparison(Comparison::NotEqual));
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
                        return self.consume_and_return(Sym::Comparison(Comparison::Equal));
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
                        "contains" => Ok(Some(Sym::Keyword(Keyword::Contains))),
                        "if" => Ok(Some(Sym::Keyword(Keyword::If))),
                        _ => Ok(Some(Sym::Id(ident))),
                    }
                }

                _ => eyre::bail!("{}: unexpected symbol '{c}'", self.text.pos()),
            },
        }
    }
}
