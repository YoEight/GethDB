use std::{iter::Peekable, str::Chars};

use crate::tokenizer::Pos;

pub struct Text<'a> {
    inner: Peekable<Chars<'a>>,
    line: u64,
    col: u64,
}

impl<'a> Text<'a> {
    pub fn new(query: &'a str) -> Self {
        Self {
            inner: query.chars().peekable(),
            line: 1,
            col: 1,
        }
    }

    pub fn shift(&mut self) -> Option<char> {
        let c = self.inner.next()?;

        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }

        Some(c)
    }

    pub fn pos(&self) -> Pos {
        Pos::new(self.line, self.col)
    }

    pub fn look_ahead(&mut self) -> Option<char> {
        self.inner.peek().copied()
    }
}
