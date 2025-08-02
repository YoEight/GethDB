use std::fmt::Display;

mod lexer;
mod text;

pub use lexer::Lexer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct Pos {
    line: u64,
    column: u64,
}

impl Display for Pos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

impl Pos {
    pub fn new(line: u64, column: u64) -> Self {
        Self { line, column }
    }

    pub fn line(&self) -> u64 {
        self.line
    }

    pub fn column(&self) -> u64 {
        self.column
    }
}
