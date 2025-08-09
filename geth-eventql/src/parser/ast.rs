use std::{collections::HashMap, fmt::Display};

use crate::{
    Pos, Type,
    sym::{Literal, Operation},
};

#[derive(Copy, Clone)]
pub struct Attributes {
    pub pos: Pos,
    pub scope: u64,
    pub tpe: Type,
}

impl Attributes {
    pub fn new(pos: Pos) -> Self {
        Self {
            pos,
            scope: u64::MAX,
            tpe: Type::Unspecified,
        }
    }
}

pub struct Query {
    pub attrs: Attributes,
    pub from_stmts: Vec<FromSource>,
    pub predicate: Option<Where>,
    pub group_by: Option<Expr>,
    pub order_by: Option<Sort>,
    pub limit: Option<Limit>,
    pub projection: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Subject {
    pub(crate) inner: Vec<String>,
}

impl Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "/")?;

        for segment in &self.inner {
            write!(f, "{segment}/")?;
        }

        Ok(())
    }
}

impl Subject {
    pub fn is_root(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn path(&self) -> &[String] {
        self.inner.as_slice()
    }
}

pub enum SourceType {
    Events,
    Subject(Subject),
    Subquery(Box<Query>),
}

impl SourceType {
    pub fn as_subject(&self) -> Option<&Subject> {
        if let Self::Subject(sub) = self {
            return Some(sub);
        }

        None
    }

    pub fn targets_events(&self) -> bool {
        if let Self::Events = self {
            return true;
        }

        false
    }
}

pub struct Source {
    pub attrs: Attributes,
    pub inner: SourceType,
}

impl Source {
    pub fn as_subquery(&self) -> Option<&Query> {
        if let SourceType::Subquery(q) = &self.inner {
            return Some(q);
        }

        None
    }
}

pub struct FromSource {
    pub attrs: Attributes,
    pub ident: String,
    pub source: Source,
}

pub struct Where {
    pub attrs: Attributes,
    pub expr: Expr,
}

pub struct Expr {
    pub attrs: Attributes,
    pub value: Value,
}

impl AsMut<Value> for Expr {
    fn as_mut(&mut self) -> &mut Value {
        &mut self.value
    }
}

impl Expr {
    pub fn as_var(&self) -> Option<&Var> {
        if let Value::Var(p) = &self.value {
            return Some(p);
        }

        None
    }

    pub fn as_binary_op(&self) -> Option<BinaryOp<'_>> {
        if let Value::Binary { lhs, op, rhs } = &self.value {
            return Some(BinaryOp {
                lhs: lhs.as_ref(),
                op: *op,
                rhs: rhs.as_ref(),
            });
        }

        None
    }

    pub fn as_unary_op(&self) -> Option<UnaryOp<'_>> {
        if let Value::Unary { op, expr } = &self.value {
            return Some(UnaryOp { op: *op, expr });
        }

        None
    }

    pub fn as_string_literal(&self) -> Option<&String> {
        if let Value::Literal(Literal::String(s)) = &self.value {
            return Some(s);
        }

        None
    }

    pub fn as_i64_literal(&self) -> Option<i64> {
        if let Value::Literal(Literal::Integral(i)) = &self.value {
            return Some(*i);
        }

        None
    }

    pub fn as_record(&self) -> Option<&Record> {
        if let Value::Record(r) = &self.value {
            return Some(r);
        }

        None
    }

    pub fn as_apply_fun(&self) -> Option<ApplyFun<'_>> {
        if let Value::App { fun, params } = &self.value {
            return Some(ApplyFun { name: fun, params });
        }

        None
    }
}

pub struct BinaryOp<'a> {
    pub lhs: &'a Expr,
    pub op: Operation,
    pub rhs: &'a Expr,
}

pub struct UnaryOp<'a> {
    pub op: Operation,
    pub expr: &'a Expr,
}

pub struct ApplyFun<'a> {
    pub name: &'a String,
    pub params: &'a Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Var {
    pub name: String,
    pub path: Vec<String>,
}

impl Display for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)?;

        for p in &self.path {
            write!(f, ".{p}")?;
        }

        Ok(())
    }
}

pub enum Value {
    Literal(Literal),
    Var(Var),
    Record(Record),
    Array(Vec<Expr>),
    App {
        fun: String,
        params: Vec<Expr>,
    },
    Binary {
        lhs: Box<Expr>,
        op: Operation,
        rhs: Box<Expr>,
    },
    Unary {
        op: Operation,
        expr: Box<Expr>,
    },
}

pub struct Record {
    pub fields: HashMap<String, Expr>,
}

impl Record {
    pub fn get(&self, id: &str) -> Option<&Expr> {
        self.fields.get(id)
    }
}

pub struct Sort {
    pub expr: Expr,
    pub order: Order,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitKind {
    Skip,
    Top,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub kind: LimitKind,
    pub value: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}
