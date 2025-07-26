use std::{
    fmt::Display,
    num::{ParseFloatError, ParseIntError},
};

use crate::{Operation, Pos, Type, Var, sym::Sym};

#[derive(Debug)]
pub struct Error {
    pos: Pos,
    kind: ErrorKind,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.pos, self.kind)
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    Lexer(LexerError),
    Parser(ParserError),
    Rename(RenameError),
    Infer(InferError),
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Lexer(e) => write!(f, "{e}"),
            ErrorKind::Parser(e) => write!(f, "{e}"),
            ErrorKind::Rename(e) => write!(f, "{e}"),
            ErrorKind::Infer(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Debug)]
pub enum LexerError {
    UnexpectedEndOfQuery,
    UnexpectedSymbol(char),
    MalformedFloatingNumber(ParseFloatError),
    MalformedIntegralNumber(ParseIntError),
    StringLiteralNotClosed,
}

#[derive(Debug)]
pub enum ParserError {
    BinaryUnaryOperationUnallowedInProjection,
    UnexpectedSymbol(Sym, Vec<Sym>),
    ExpectedGreaterOrEqualToZero(i64),
    ExpectedIdent(Sym),
    ExpectedSource(Sym),
    ExpectedExpr(Sym),
}

#[derive(Debug)]
pub enum RenameError {
    VariableAlreadyExist(String),
    VariableDoesNotExist(String),
    OnlyDataFieldDynAccessField,
}

#[derive(Debug)]
pub enum InferError {
    TypeMismatch(Type, Type),
    VarTypeMismatch(Var, Type, Type),
    UnsupportedBinaryOperation(Operation),
}

impl Display for LexerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LexerError::UnexpectedEndOfQuery => write!(f, "unexpected end of the query"),
            LexerError::UnexpectedSymbol(sym) => write!(f, "unexpected symbol '{sym}'"),
            LexerError::MalformedFloatingNumber(e) => write!(f, "malformed floating number: {e}"),
            LexerError::MalformedIntegralNumber(e) => write!(f, "malformed integral number: {e}"),
            LexerError::StringLiteralNotClosed => {
                write!(f, "string literal is not closed properly")
            }
        }
    }
}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserError::BinaryUnaryOperationUnallowedInProjection => write!(
                f,
                "binary or unary operations are not allowed when projecting a result"
            ),

            ParserError::UnexpectedSymbol(got, expected) => {
                if expected.is_empty() {
                    write!(f, "unexpected symbol {got}")
                } else {
                    write!(f, "expected {expected:?} but found '{got}' instead")
                }
            }

            ParserError::ExpectedGreaterOrEqualToZero(x) => {
                write!(
                    f,
                    "expected a greater or equal to 0 integer but found '{x}' instead"
                )
            }

            ParserError::ExpectedIdent(sym) => {
                write!(f, "expected an ident but got '{sym}' instead")
            }

            ParserError::ExpectedSource(sym) => {
                write!(f, "expected a source but got '{sym}' instead")
            }

            ParserError::ExpectedExpr(sym) => {
                write!(f, "expected an expression but got '{sym}' instead")
            }
        }
    }
}

impl Display for RenameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RenameError::VariableAlreadyExist(x) => {
                write!(f, "variable '{x}' already exists in this scope")
            }

            RenameError::VariableDoesNotExist(x) => {
                write!(f, "variable '{x}' does not exist in this scope")
            }

            RenameError::OnlyDataFieldDynAccessField => write!(
                f,
                "only the 'data' field can have dynamically accessed fields"
            ),
        }
    }
}

impl Display for InferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InferError::TypeMismatch(x, y) => {
                write!(f, "expected type '{x}' but got '{y}' instead")
            }

            InferError::VarTypeMismatch(var, x, y) => write!(
                f,
                "'{var}' type was expected to be '{x}' but got '{y}' instead"
            ),

            InferError::UnsupportedBinaryOperation(op) => {
                write!(f, "'{op}' is not supported for binary operations")
            }
        }
    }
}
