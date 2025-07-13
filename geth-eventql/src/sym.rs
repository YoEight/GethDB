use std::fmt::Display;

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
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::String(s) => write!(f, "\"{s}\""),
            Literal::Integral(n) => write!(f, "{n}"),
            Literal::Float(float) => write!(f, "{float}"),
            Literal::Bool(b) => write!(f, "{b}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Sym {
    EOF,
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
    DoubleQuote,
    SingleQuote,
}

impl Display for Sym {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Sym::EOF => write!(f, "<eof>"),
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
            Sym::DoubleQuote => write!(f, "\""),
            Sym::SingleQuote => write!(f, "'"),
        }
    }
}
