use std::{collections::HashMap, fmt::Display, mem, ptr::NonNull};

use crate::{
    Pos, Type,
    sym::{Literal, Operation},
};

pub struct Query<A> {
    pub tag: A,
    pub from_stmts: Vec<From<A>>,
    pub predicate: Option<Where<A>>,
    pub group_by: Option<Expr<A>>,
    pub order_by: Option<Sort<A>>,
    pub limit: Option<Limit>,
    pub projection: Expr<A>,
}

pub struct NewQuery {
    pub attrs: Attributes,
    pub from_stmts: Vec<From<A>>,
    pub predicate: Option<Where<A>>,
    pub group_by: Option<Expr<A>>,
    pub order_by: Option<Sort<A>>,
    pub limit: Option<Limit>,
    pub projection: Expr<A>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, Default)]
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

pub enum SourceType<A> {
    Events,
    Subject(Subject),
    Subquery(Box<Query<A>>),
}

impl<A> SourceType<A> {
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

pub struct Source<A> {
    pub tag: A,
    pub inner: SourceType<A>,
}

impl<A> Source<A> {
    pub fn as_subquery(&self) -> Option<&Query<A>> {
        if let SourceType::Subquery(q) = &self.inner {
            return Some(q);
        }

        None
    }
}

pub struct From<A> {
    pub tag: A,
    pub ident: String,
    pub source: Source<A>,
}

pub struct NewFrom {
    pub attrs: Attributes,
    pub ident: String,
    pub source: Source<A>,
}

pub struct Where<A> {
    pub tag: A,
    pub expr: Expr<A>,
}

pub struct Expr<A> {
    pub tag: A,
    pub value: Value<A>,
}

impl<A> AsMut<Value<A>> for Expr<A> {
    fn as_mut(&mut self) -> &mut Value<A> {
        &mut self.value
    }
}

impl<A> Expr<A> {
    pub fn as_var(&self) -> Option<&Var> {
        if let Value::Var(p) = &self.value {
            return Some(p);
        }

        None
    }

    pub fn as_binary_op(&self) -> Option<BinaryOp<'_, A>> {
        if let Value::Binary { lhs, op, rhs } = &self.value {
            return Some(BinaryOp {
                lhs: lhs.as_ref(),
                op: *op,
                rhs: rhs.as_ref(),
            });
        }

        None
    }

    pub fn as_unary_op(&self) -> Option<UnaryOp<'_, A>> {
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

    pub fn as_record(&self) -> Option<&Record<A>> {
        if let Value::Record(r) = &self.value {
            return Some(r);
        }

        None
    }

    pub fn as_apply_fun(&self) -> Option<ApplyFun<'_, A>> {
        if let Value::App { fun, params } = &self.value {
            return Some(ApplyFun { name: fun, params });
        }

        None
    }
}

pub struct BinaryOp<'a, A> {
    pub lhs: &'a Expr<A>,
    pub op: Operation,
    pub rhs: &'a Expr<A>,
}

pub struct UnaryOp<'a, A> {
    pub op: Operation,
    pub expr: &'a Expr<A>,
}

pub struct ApplyFun<'a, A> {
    pub name: &'a String,
    pub params: &'a Vec<Expr<A>>,
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

#[derive(Default)]
pub struct Attributes {
    pub pos: Pos,
    pub typ: Type,
    pub scope: u64,
}

#[derive(Default)]
pub struct NodeValue {
    pub str_val: String,
    pub int_val: i64,
    pub float_val: f64,
    pub bool_val: bool,
    pub sub_val: Subject,
}

pub enum NodeKind {
    Literal,
    Var,
    Record,
    Array,
    App,
    Operation,
}

pub struct Node {
    pub attrs: Attributes,
    pub kind: NodeKind,
    pub value: NodeValue,
    pub size: usize,
    pub descendants: Vec<Node>,
}

struct NTState {
    node: NonNull<Node>,
    visited: bool,
}

impl NTState {
    fn new(node: NonNull<Node>) -> Self {
        Self {
            node,
            visited: false,
        }
    }
}

impl Node {
    pub fn dfs<V>(&mut self, visitor: &mut V) -> crate::Result<()>
    where
        V: NodeVisitor,
    {
        let mut stack = vec![NTState::new(NonNull::from(self))];

        while let Some(mut item) = stack.pop() {
            let node = unsafe { item.node.as_mut() };

            if node.is_leaf() {
                visitor.on_leaf(node)?;
                continue;
            }

            if node.is_parent() && item.visited {
                visitor.on_node(node)?;
                continue;
            }

            item.visited = true;
            stack.push(item);

            for desc in node.descendants.iter_mut() {
                stack.push(NTState::new(NonNull::from(desc)));
            }
        }

        Ok(())
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self.kind, NodeKind::Literal | NodeKind::Var)
    }

    pub fn is_parent(&self) -> bool {
        !self.is_leaf()
    }
}

pub trait NodeVisitor {
    fn on_node(&mut self, node: &mut Node) -> crate::Result<()>;
    fn on_leaf(&mut self, leaf: &mut Node) -> crate::Result<()>;
}

pub enum Value<A> {
    Literal(Literal),
    Var(Var),
    Record(Record<A>),
    Array(Vec<Expr<A>>),
    App {
        fun: String,
        params: Vec<Expr<A>>,
    },
    Binary {
        lhs: Box<Expr<A>>,
        op: Operation,
        rhs: Box<Expr<A>>,
    },
    Unary {
        op: Operation,
        expr: Box<Expr<A>>,
    },
}

pub struct Record<A> {
    pub fields: HashMap<String, Expr<A>>,
}

impl<A> Record<A> {
    pub fn get(&self, id: &str) -> Option<&Expr<A>> {
        self.fields.get(id)
    }
}

pub struct Sort<A> {
    pub expr: Expr<A>,
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
