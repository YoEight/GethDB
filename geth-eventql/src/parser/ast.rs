use std::{collections::HashMap, fmt::Display, ptr::NonNull};

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

impl Query {
    pub fn dfs_post_order<V: QueryVisitor>(&mut self, visitor: &mut V) -> crate::Result<()> {
        query_dfs_post_order(vec![NT::new(self)], visitor)
    }
}

fn query_dfs_post_order<V: QueryVisitor>(
    mut stack: Vec<NT<Query>>,
    visitor: &mut V,
) -> crate::Result<()> {
    while let Some(mut item) = stack.pop() {
        let query = unsafe { item.node.as_mut() };

        if !item.visited {
            item.visited = true;
            stack.push(item);

            visitor.enter_query()?;

            for from_stmt in query.from_stmts.iter_mut() {
                visitor.enter_from(&mut from_stmt.attrs, &from_stmt.ident)?;

                match &mut from_stmt.source.inner {
                    SourceType::Events => {
                        visitor.on_source_events(&mut from_stmt.attrs, &from_stmt.ident)?
                    }

                    SourceType::Subject(subject) => visitor.on_source_subject(
                        &mut from_stmt.attrs,
                        &from_stmt.ident,
                        subject,
                    )?,

                    SourceType::Subquery(sub_query) => {
                        if visitor.on_source_subquery(&mut from_stmt.attrs, &from_stmt.ident)? {
                            stack.push(NT::new(sub_query));
                        }
                    }
                }

                visitor.exit_source(&mut from_stmt.source.attrs)?;
                visitor.exit_from(&mut from_stmt.attrs, &from_stmt.ident)?;
            }

            continue;
        }

        if let Some(predicate) = query.predicate.as_mut() {
            visitor.enter_where_clause(&mut predicate.attrs, &mut predicate.expr)?;
            on_expr(visitor, &mut predicate.expr)?;
            visitor.exit_where_clause(&mut predicate.attrs, &mut predicate.expr)?;
        }

        if let Some(expr) = query.group_by.as_mut() {
            visitor.enter_group_by(expr)?;
            on_expr(visitor, expr)?;
            visitor.leave_group_by(expr)?;
        }

        if let Some(sort) = query.order_by.as_mut() {
            visitor.enter_order_by(&mut sort.order, &mut sort.expr)?;
            on_expr(visitor, &mut sort.expr)?;
            visitor.leave_order_by(&mut sort.order, &mut sort.expr)?;
        }

        visitor.enter_projection(&mut query.projection)?;
        on_expr(visitor, &mut query.projection)?;
        visitor.leave_projection(&mut query.projection)?;
        visitor.exit_query()?;
    }

    Ok(())
}

fn on_expr<V: QueryVisitor>(visitor: &mut V, expr: &mut Expr) -> crate::Result<()> {
    let mut expr_visitor = visitor.expr_visitor();
    expr.dfs_post_order(&mut expr_visitor)
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

    pub fn dfs_post_order<V: ExprVisitor>(&mut self, visitor: &mut V) -> crate::Result<()> {
        let mut stack = vec![NT::new(self)];

        while let Some(mut item) = stack.pop() {
            let node = unsafe { item.node.as_mut() };

            match &mut node.value {
                Value::Literal(lit) => {
                    visitor.on_literal(&mut node.attrs, lit)?;
                }

                Value::Var(var) => {
                    visitor.on_var(&mut node.attrs, var)?;
                }

                Value::Record(record) => {
                    if item.visited {
                        visitor.on_record(&mut node.attrs, record)?;
                        continue;
                    }

                    item.visited = true;
                    stack.push(item);

                    for expr in record.fields.values_mut() {
                        stack.push(NT::new(expr));
                    }
                }

                Value::Array(exprs) => {
                    if item.visited {
                        visitor.on_array(&mut node.attrs, exprs)?;
                        continue;
                    }

                    item.visited = true;
                    stack.push(item);

                    for expr in exprs.iter_mut().rev() {
                        stack.push(NT::new(expr));
                    }
                }

                Value::App { fun, params } => {
                    if item.visited {
                        visitor.exit_app(&mut node.attrs, fun, params)?;
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_app(&mut node.attrs, fun, params)?;
                    stack.push(item);

                    for param in params.iter_mut().rev() {
                        stack.push(NT::new(param));
                    }
                }

                Value::Binary { lhs, op, rhs } => {
                    if item.visited {
                        visitor.exit_binary_op(&mut node.attrs, op, lhs, rhs)?;
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_binary_op(&mut node.attrs, op, lhs, rhs)?;
                    stack.push(item);
                    stack.push(NT::new(rhs));
                    stack.push(NT::new(lhs));
                }

                Value::Unary { op, expr } => {
                    if item.visited {
                        visitor.exit_unary_op(&mut node.attrs, op, expr)?;
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_unary_op(&mut node.attrs, op, expr)?;
                    stack.push(item);
                    stack.push(NT::new(expr));
                }
            }
        }

        Ok(())
    }
}

struct NT<A> {
    node: NonNull<A>,
    visited: bool,
}

impl<A> NT<A> {
    fn new(node: &mut A) -> Self {
        Self {
            node: NonNull::from(node),
            visited: false,
        }
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

#[allow(dead_code)]
pub enum Context {
    Unspecified,
    Where,
    GroupBy,
    OrderBy,
    Projection,
}

#[allow(unused_variables)]
pub trait QueryVisitor {
    type Inner<'a>: ExprVisitor
    where
        Self: 'a;

    fn enter_query(&mut self) -> crate::Result<()> {
        Ok(())
    }

    fn exit_query(&mut self) -> crate::Result<()> {
        Ok(())
    }

    fn enter_from(&mut self, attrs: &mut Attributes, ident: &str) -> crate::Result<()> {
        Ok(())
    }

    fn exit_from(&mut self, attrs: &mut Attributes, ident: &str) -> crate::Result<()> {
        Ok(())
    }

    fn on_source_events(&mut self, attrs: &mut Attributes, ident: &str) -> crate::Result<()> {
        Ok(())
    }

    fn on_source_subject(
        &mut self,
        attrs: &mut Attributes,
        ident: &str,
        subject: &mut Subject,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn on_source_subquery(&mut self, attrs: &mut Attributes, ident: &str) -> crate::Result<bool> {
        Ok(true)
    }

    fn exit_source(&mut self, _attrs: &mut Attributes) -> crate::Result<()> {
        Ok(())
    }

    fn enter_where_clause(&mut self, attrs: &mut Attributes, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn exit_where_clause(&mut self, attrs: &mut Attributes, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn enter_group_by(&mut self, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn leave_group_by(&mut self, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn enter_order_by(&mut self, order: &mut Order, _expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn leave_order_by(&mut self, order: &mut Order, _expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn enter_projection(&mut self, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn leave_projection(&mut self, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn expr_visitor<'a>(&'a mut self) -> Self::Inner<'a>;
}

#[allow(unused_variables)]
pub trait ExprVisitor {
    fn on_literal(&mut self, attrs: &mut Attributes, lit: &mut Literal) -> crate::Result<()> {
        Ok(())
    }

    fn on_var(&mut self, attrs: &mut Attributes, var: &mut Var) -> crate::Result<()> {
        Ok(())
    }

    fn on_record(&mut self, attrs: &mut Attributes, record: &mut Record) -> crate::Result<()> {
        Ok(())
    }

    fn on_array(&mut self, attrs: &mut Attributes, values: &mut Vec<Expr>) -> crate::Result<()> {
        Ok(())
    }

    fn enter_app(
        &mut self,
        attrs: &mut Attributes,
        name: &str,
        params: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_app(
        &mut self,
        attrs: &mut Attributes,
        name: &str,
        params: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn enter_binary_op(
        &mut self,
        attrs: &mut Attributes,
        op: &Operation,
        lhs: &mut Expr,
        rhs: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_binary_op(
        &mut self,
        attrs: &mut Attributes,
        op: &Operation,
        lhs: &mut Expr,
        rhs: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn enter_unary_op(
        &mut self,
        attrs: &mut Attributes,
        op: &Operation,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_unary_op(
        &mut self,
        attrs: &mut Attributes,
        op: &Operation,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }
}
