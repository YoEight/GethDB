use crate::{sym::Sym, tokenizer::text::Text};

pub struct Lexer<'a> {
    text: Text<'a>,
}

impl<'a> Lexer<'a> {
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
                _ => eyre::bail!("{}: unexpected symbol '{c}'", self.text.pos()),
            },
        }
    }
}
