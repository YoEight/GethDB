use std::fmt::Display;

use crate::Subject;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    From,
    In,
    Where,
    Order,
    By,
    Asc,
    Desc,
    Group,
    Skip,
    Top,
    Project,
    Into,
    Distinct,
    Having,
    As,
    If,
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Keyword::From => write!(f, "FROM"),
            Keyword::In => write!(f, "In"),
            Keyword::Where => write!(f, "WHERE"),
            Keyword::Order => write!(f, "ORDER"),
            Keyword::By => write!(f, "BY"),
            Keyword::Asc => write!(f, "ASC"),
            Keyword::Desc => write!(f, "DESC"),
            Keyword::Group => write!(f, "GROUP"),
            Keyword::Skip => write!(f, "SKIP"),
            Keyword::Top => write!(f, "TOP"),
            Keyword::Project => write!(f, "PROJECT"),
            Keyword::Into => write!(f, "INTO"),
            Keyword::Distinct => write!(f, "DISTINCT"),
            Keyword::Having => write!(f, "HAVING"),
            Keyword::As => write!(f, "AS"),
            Keyword::If => write!(f, "IF"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    And,
    Or,
    Xor,
    Not,
    Contains,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
            Self::Xor => write!(f, "XOR"),
            Self::Not => write!(f, "NOT"),
            Self::Contains => write!(f, "CONTAINS"),
            Self::Equal => write!(f, "=="),
            Self::NotEqual => write!(f, "!="),
            Self::LessThan => write!(f, "<"),
            Self::GreaterThan => write!(f, ">"),
            Self::LessThanOrEqual => write!(f, "<="),
            Self::GreaterThanOrEqual => write!(f, ">="),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
    Integral(i64),
    Float(f64),
    Bool(bool),
    Subject(Subject),
}

impl Literal {
    pub fn is_string(&self) -> bool {
        self.as_str().is_some()
    }

    pub fn as_str(&self) -> Option<&str> {
        if let Self::String(s) = self {
            return Some(s.as_str());
        }

        None
    }
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(x), Self::String(y)) => x == y,
            (Self::Integral(x), Self::Integral(y)) => x == y,
            (Self::Subject(x), Self::Subject(y)) => x == y,
            (Self::Bool(x), Self::Bool(y)) => x == y,
            _ => false,
        }
    }
}

impl Eq for Literal {}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::String(s) => write!(f, "\"{s}\""),
            Literal::Integral(n) => write!(f, "{n}"),
            Literal::Float(float) => write!(f, "{float}"),
            Literal::Bool(b) => write!(f, "{b}"),
            Literal::Subject(sub) => write!(f, "{sub}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sym {
    Id(String),
    Keyword(Keyword),
    Operation(Operation),
    Literal(Literal),
    Whitespace,
    Dot,
    LParens,
    RParens,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Colon,
}

impl Display for Sym {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Sym::Id(id) => write!(f, "{id}"),
            Sym::Keyword(keyword) => write!(f, "{keyword}"),
            Sym::Operation(op) => write!(f, "{op}"),
            Sym::Literal(literal) => write!(f, "{literal}"),
            Sym::Whitespace => write!(f, " "),
            Sym::Dot => write!(f, "."),
            Sym::LParens => write!(f, "("),
            Sym::RParens => write!(f, ")"),
            Sym::LBrace => write!(f, "{{"),
            Sym::RBrace => write!(f, "}}"),
            Sym::LBracket => write!(f, "["),
            Sym::RBracket => write!(f, "]"),
            Sym::Comma => write!(f, ","),
            Sym::Colon => write!(f, ":"),
        }
    }
}

impl From<Keyword> for Sym {
    fn from(value: Keyword) -> Self {
        Self::Keyword(value)
    }
}
