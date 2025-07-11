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
    Contains,
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
            Keyword::Contains => write!(f, "CONTAINS"),
            Keyword::If => write!(f, "IF"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Logical {
    And,
    Or,
    Xor,
    Not,
}

impl Display for Logical {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Logical::And => write!(f, "AND"),
            Logical::Or => write!(f, "OR"),
            Logical::Xor => write!(f, "XOR"),
            Logical::Not => write!(f, "NOT"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

impl Display for Comparison {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Comparison::Equal => write!(f, "=="),
            Comparison::NotEqual => write!(f, "!="),
            Comparison::LessThan => write!(f, "<"),
            Comparison::GreaterThan => write!(f, ">"),
            Comparison::LessThanOrEqual => write!(f, "<="),
            Comparison::GreaterThanOrEqual => write!(f, ">="),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sym {
    EOF,
    Id(String),
    Type(String),
    Keyword(Keyword),
    Logical(Logical),
    Comparison(Comparison),
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
            Sym::Type(typ) => write!(f, "{typ}"),
            Sym::Keyword(keyword) => write!(f, "{keyword}"),
            Sym::Logical(logical) => write!(f, "{logical}"),
            Sym::Comparison(comparison) => write!(f, "{comparison}"),
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
