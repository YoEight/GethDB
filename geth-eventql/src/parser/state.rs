use crate::{
    sym::Sym,
    tokenizer::{Lexer, Pos},
};

struct LookAhead {
    sym: Sym,
    pos: Pos,
}

pub struct ParserState<'a> {
    lexer: Lexer<'a>,
    buf: Option<LookAhead>,
}

impl<'a> From<Lexer<'a>> for ParserState<'a> {
    fn from(lexer: Lexer<'a>) -> Self {
        Self { lexer, buf: None }
    }
}

impl<'a> ParserState<'a> {
    pub fn new(lexer: Lexer<'a>) -> Self {
        Self { lexer, buf: None }
    }

    pub fn look_ahead(&mut self) -> eyre::Result<Option<&Sym>> {
        if self.buf.is_none() {
            let pos = self.lexer.pos();
            self.buf = self.lexer.next_sym()?.map(|sym| LookAhead { sym, pos });
        }

        Ok(self.buf.as_ref().map(|x| &x.sym))
    }

    pub fn shift(&mut self) -> eyre::Result<Option<Sym>> {
        if self.buf.is_none() {
            return self.lexer.next_sym();
        }

        Ok(self.buf.take().map(|x| x.sym))
    }

    pub fn shift_or_bail(&mut self) -> eyre::Result<Sym> {
        if let Some(sym) = self.shift()? {
            return Ok(sym);
        }

        eyre::bail!("unexpected end of query")
    }

    pub fn skip_whitespace(&mut self) -> eyre::Result<()> {
        let sym_opt = self.look_ahead()?;

        if let Some(sym) = sym_opt {
            if sym != &Sym::Whitespace {
                return Ok(());
            }

            self.shift()?;
        }

        Ok(())
    }

    pub fn expect(&mut self, exp: Sym) -> eyre::Result<()> {
        let pos = self.pos();
        let sym = self.shift_or_bail()?;

        if sym != exp {
            eyre::bail!("{pos}: expected {exp} but got {sym} instead");
        }

        Ok(())
    }

    pub fn pos(&self) -> Pos {
        if let Some(x) = &self.buf {
            return x.pos;
        }

        self.lexer.pos()
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::state::ParserState, sym::Sym, tokenizer::Lexer};

    #[test]
    fn test_look_ahead() -> eyre::Result<()> {
        let mut state: ParserState<'_> = Lexer::new("foobar where").into();

        let sym = state.look_ahead()?.cloned();

        assert_eq!(Some(Sym::Id("foobar".to_string())), sym);
        assert_eq!(Some(Sym::Id("foobar".to_string())), sym);

        Ok(())
    }

    #[test]
    fn test_shift() -> eyre::Result<()> {
        let mut state: ParserState<'_> = Lexer::new("foobar where").into();

        let sym = state.shift_or_bail()?;

        assert_eq!(Sym::Id("foobar".to_string()), sym);
        assert_eq!(Sym::Whitespace, state.shift_or_bail()?);

        Ok(())
    }
}
