use std::{collections::VecDeque, fmt::Display, ptr::NonNull};

use crate::{
    Pos, Type,
    sym::{Literal, Operation},
};

#[derive(Copy, Clone)]
pub struct NodeAttributes {
    pub pos: Pos,
    pub scope: u64,
    pub tpe: Type,
}

impl NodeAttributes {
    pub fn new(pos: Pos) -> Self {
        Self {
            pos,
            scope: u64::MAX,
            tpe: Type::Unspecified,
        }
    }
}

pub struct Query {
    pub attrs: NodeAttributes,
    pub from_stmts: Vec<FromSource>,
    pub predicate: Option<Where>,
    pub group_by: Option<Expr>,
    pub order_by: Option<Sort>,
    pub limit: Option<Limit>,
    pub projection: Expr,
}

impl Query {
    pub fn dfs_post_order_mut<V: QueryVisitorMut>(&mut self, visitor: &mut V) -> crate::Result<()> {
        query_dfs_post_order_mut(vec![ItemMut::new(self)], visitor)
    }

    pub fn dfs_post_order<V: QueryVisitor>(&self, visitor: &mut V) {
        query_dfs_post_order(vec![Item::new(self)], visitor);
    }

    pub fn dfs_pre_order<V: QueryVisitor>(&self, visitor: &mut V) {
        let mut queue = VecDeque::new();
        queue.push_back(self);

        query_dfs_pre_order(queue, visitor);
    }
}

fn query_dfs_post_order_mut<V: QueryVisitorMut>(
    mut stack: Vec<ItemMut<Query>>,
    visitor: &mut V,
) -> crate::Result<()> {
    while let Some(mut item) = stack.pop() {
        let query = unsafe { item.value.as_mut() };

        if !item.visited {
            item.visited = true;
            stack.push(item);

            visitor.enter_query_mut()?;

            for from_stmt in query.from_stmts.iter_mut() {
                visitor.enter_from_mut(&mut from_stmt.attrs, &from_stmt.ident)?;

                match &mut from_stmt.source.inner {
                    SourceType::Events => {
                        visitor.on_source_events_mut(&mut from_stmt.attrs, &from_stmt.ident)?
                    }

                    SourceType::Subject(subject) => visitor.on_source_subject_mut(
                        &mut from_stmt.attrs,
                        &from_stmt.ident,
                        subject,
                    )?,

                    SourceType::Subquery(sub_query) => {
                        if visitor.on_source_subquery_mut(&mut from_stmt.attrs, &from_stmt.ident)? {
                            stack.push(ItemMut::new(sub_query));
                        }
                    }
                }

                visitor.exit_source_mut(&mut from_stmt.source.attrs)?;
                visitor.exit_from_mut(&mut from_stmt.attrs, &from_stmt.ident)?;
            }

            continue;
        }

        if let Some(predicate) = query.predicate.as_mut() {
            visitor.enter_where_clause_mut(&mut predicate.attrs, &mut predicate.expr)?;
            on_expr_mut(visitor, &mut predicate.expr)?;
            visitor.exit_where_clause_mut(&mut predicate.attrs, &mut predicate.expr)?;
        }

        if let Some(expr) = query.group_by.as_mut() {
            visitor.enter_group_by_mut(expr)?;
            on_expr_mut(visitor, expr)?;
            visitor.leave_group_by_mut(expr)?;
        }

        if let Some(sort) = query.order_by.as_mut() {
            visitor.enter_order_by_mut(&mut sort.order, &mut sort.expr)?;
            on_expr_mut(visitor, &mut sort.expr)?;
            visitor.leave_order_by_mut(&mut sort.order, &mut sort.expr)?;
        }

        visitor.enter_projection_mut(&mut query.projection)?;
        on_expr_mut(visitor, &mut query.projection)?;
        visitor.leave_projection_mut(&mut query.projection)?;
        visitor.exit_query_mut()?;
    }

    Ok(())
}

fn on_expr_mut<V: QueryVisitorMut>(visitor: &mut V, expr: &mut Expr) -> crate::Result<()> {
    let mut expr_visitor = visitor.expr_visitor_mut();
    expr.dfs_post_order_mut(&mut expr_visitor)
}

fn query_dfs_pre_order<V: QueryVisitor>(mut queue: VecDeque<&Query>, visitor: &mut V) {
    while let Some(query) = queue.pop_front() {
        visitor.enter_query(&query.attrs);

        for from_stmt in query.from_stmts.iter() {
            visitor.enter_from(&from_stmt.attrs, &from_stmt.ident);

            match &from_stmt.source.inner {
                SourceType::Events => visitor.on_source_events(&from_stmt.attrs, &from_stmt.ident),

                SourceType::Subject(subject) => {
                    visitor.on_source_subject(&from_stmt.attrs, &from_stmt.ident, subject)
                }

                SourceType::Subquery(query) => {
                    if visitor.on_source_subquery(&from_stmt.attrs, &from_stmt.ident, query) {
                        queue.push_back(query);
                    }
                }
            }

            visitor.exit_source(&from_stmt.source.attrs);
            visitor.exit_from(&from_stmt.attrs, &from_stmt.ident);
        }

        if let Some(predicate) = query.predicate.as_ref() {
            visitor.enter_where_clause(&predicate.attrs, &predicate.expr);
            on_expr(visitor, &predicate.expr);
            visitor.exit_where_clause(&predicate.attrs, &predicate.expr);
        }

        if let Some(expr) = query.group_by.as_ref() {
            visitor.enter_group_by(expr);
            on_expr(visitor, expr);
            visitor.leave_group_by(expr);
        }

        if let Some(sort) = query.order_by.as_ref() {
            visitor.enter_order_by(&sort.order, &sort.expr);
            on_expr(visitor, &sort.expr);
            visitor.leave_order_by(&sort.order, &sort.expr);
        }

        visitor.enter_projection(&query.projection);
        on_expr(visitor, &query.projection);
        visitor.leave_projection(&query.projection);
        visitor.exit_query();
    }
}

fn query_dfs_post_order<V: QueryVisitor>(mut stack: Vec<Item<Query>>, visitor: &mut V) {
    while let Some(mut item) = stack.pop() {
        let query = item.value;
        if !item.visited {
            visitor.enter_query(&item.value.attrs);
            item.visited = true;
            stack.push(item);

            for from_stmt in query.from_stmts.iter() {
                visitor.enter_from(&from_stmt.attrs, &from_stmt.ident);

                match &from_stmt.source.inner {
                    SourceType::Events => {
                        visitor.on_source_events(&from_stmt.attrs, &from_stmt.ident)
                    }

                    SourceType::Subject(subject) => {
                        visitor.on_source_subject(&from_stmt.attrs, &from_stmt.ident, subject)
                    }

                    SourceType::Subquery(sub_query) => {
                        if visitor.on_source_subquery(&from_stmt.attrs, &from_stmt.ident, sub_query)
                        {
                            stack.push(Item::new(sub_query));
                        }
                    }
                }

                visitor.exit_source(&from_stmt.source.attrs);
                visitor.exit_from(&from_stmt.attrs, &from_stmt.ident);
            }

            continue;
        }

        if let Some(predicate) = query.predicate.as_ref() {
            visitor.enter_where_clause(&predicate.attrs, &predicate.expr);
            on_expr(visitor, &predicate.expr);
            visitor.exit_where_clause(&predicate.attrs, &predicate.expr);
        }

        if let Some(expr) = query.group_by.as_ref() {
            visitor.enter_group_by(expr);
            on_expr(visitor, expr);
            visitor.leave_group_by(expr);
        }

        if let Some(sort) = query.order_by.as_ref() {
            visitor.enter_order_by(&sort.order, &sort.expr);
            on_expr(visitor, &sort.expr);
            visitor.leave_order_by(&sort.order, &sort.expr);
        }

        visitor.enter_projection(&query.projection);
        on_expr(visitor, &query.projection);
        visitor.leave_projection(&query.projection);
        visitor.exit_query();
    }
}

fn on_expr<V: QueryVisitor>(visitor: &mut V, expr: &Expr) {
    let mut expr_visitor = visitor.expr_visitor();
    expr.dfs_post_order(&mut expr_visitor);
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
    pub attrs: NodeAttributes,
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
    pub attrs: NodeAttributes,
    pub ident: String,
    pub source: Source,
}

pub struct Where {
    pub attrs: NodeAttributes,
    pub expr: Expr,
}

#[derive(Clone)]
pub struct Expr {
    pub attrs: NodeAttributes,
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

    pub fn as_record(&self) -> Option<Rec<'_>> {
        if let Value::Record(inner) = &self.value {
            return Some(Rec { inner });
        }

        None
    }

    pub fn as_apply_fun(&self) -> Option<ApplyFun<'_>> {
        if let Value::App { fun, params } = &self.value {
            return Some(ApplyFun { name: fun, params });
        }

        None
    }

    pub fn dfs_post_order_mut<V: ExprVisitorMut>(&mut self, visitor: &mut V) -> crate::Result<()> {
        let mut stack = vec![ItemMut::new(self)];

        while let Some(mut item) = stack.pop() {
            let node = unsafe { item.value.as_mut() };

            match &mut node.value {
                Value::Literal(lit) => {
                    visitor.on_literal(&mut node.attrs, lit)?;
                }

                Value::Var(var) => {
                    visitor.on_var(&mut node.attrs, var)?;
                }

                Value::Field { label, value } => {
                    if item.visited {
                        visitor.exit_field(&mut node.attrs, label.as_mut_str(), value.as_mut())?;
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_field(&mut node.attrs, label.as_mut_str(), value.as_mut())?;
                    stack.push(item);
                    stack.push(ItemMut::new(value.as_mut()));
                }

                Value::Record(record) => {
                    if item.visited {
                        visitor.exit_record(&mut node.attrs, record)?;
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_record(&mut node.attrs, record)?;
                    stack.push(item);

                    for expr in record.iter_mut().rev() {
                        stack.push(ItemMut::new(expr));
                    }
                }

                Value::Array(exprs) => {
                    if item.visited {
                        visitor.exit_array(&mut node.attrs, exprs)?;
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_array(&mut node.attrs, exprs)?;
                    stack.push(item);

                    for expr in exprs.iter_mut().rev() {
                        stack.push(ItemMut::new(expr));
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
                        stack.push(ItemMut::new(param));
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
                    stack.push(ItemMut::new(rhs));
                    stack.push(ItemMut::new(lhs));
                }

                Value::Unary { op, expr } => {
                    if item.visited {
                        visitor.exit_unary_op(&mut node.attrs, op, expr)?;
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_unary_op(&mut node.attrs, op, expr)?;
                    stack.push(item);
                    stack.push(ItemMut::new(expr));
                }
            }
        }

        Ok(())
    }

    pub fn dfs_post_order<V: ExprVisitor>(&self, visitor: &mut V) {
        let mut stack = vec![Item::new(self)];

        while let Some(mut item) = stack.pop() {
            match &item.value.value {
                Value::Literal(lit) => {
                    visitor.on_literal(&item.value.attrs, lit);
                }

                Value::Var(var) => {
                    visitor.on_var(&item.value.attrs, var);
                }

                Value::Field { label, value } => {
                    if item.visited {
                        visitor.exit_field(&item.value.attrs, label.as_str(), value);
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_field(&item.value.attrs, label.as_str(), value);
                    stack.push(item);
                    stack.push(Item::new(value));
                }

                Value::Record(record) => {
                    if item.visited {
                        visitor.exit_record(&item.value.attrs, record);
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_record(&item.value.attrs, record);
                    stack.push(item);

                    for expr in record.iter().rev() {
                        stack.push(Item::new(expr));
                    }
                }

                Value::Array(exprs) => {
                    if item.visited {
                        visitor.exit_array(&item.value.attrs, exprs);
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_array(&item.value.attrs, exprs);
                    stack.push(item);

                    for expr in exprs.iter().rev() {
                        stack.push(Item::new(expr));
                    }
                }

                Value::App { fun, params } => {
                    if item.visited {
                        visitor.exit_app(&item.value.attrs, fun, params);
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_app(&item.value.attrs, fun, params);
                    stack.push(item);

                    for param in params.iter().rev() {
                        stack.push(Item::new(param));
                    }
                }

                Value::Binary { lhs, op, rhs } => {
                    if item.visited {
                        visitor.exit_binary_op(&item.value.attrs, op, lhs, rhs);
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_binary_op(&item.value.attrs, op, lhs, rhs);
                    stack.push(item);
                    stack.push(Item::new(rhs));
                    stack.push(Item::new(lhs));
                }

                Value::Unary { op, expr } => {
                    if item.visited {
                        visitor.exit_unary_op(&item.value.attrs, op, expr);
                        continue;
                    }

                    item.visited = true;
                    visitor.enter_unary_op(&item.value.attrs, op, expr);
                    stack.push(item);
                    stack.push(Item::new(expr));
                }
            }
        }
    }
}

struct ItemMut<A> {
    value: NonNull<A>,
    visited: bool,
}

impl<A> ItemMut<A> {
    fn new(node: &mut A) -> Self {
        Self {
            value: NonNull::from(node),
            visited: false,
        }
    }
}

struct Item<'a, A> {
    value: &'a A,
    visited: bool,
}

impl<'a, A> Item<'a, A> {
    fn new(node: &'a A) -> Self {
        Self {
            value: node,
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

pub struct Rec<'a> {
    inner: &'a Vec<Expr>,
}

impl Rec<'_> {
    pub fn get(&self, id: &str) -> Option<&Expr> {
        for expr in self.inner.iter() {
            if let Value::Field { label, value } = &expr.value
                && label == id
            {
                return Some(value);
            }
        }

        None
    }
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

#[derive(Clone)]
pub enum Value {
    Literal(Literal),

    Var(Var),

    Field {
        label: String,
        value: Box<Expr>,
    },

    Record(Vec<Expr>),

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

#[derive(PartialEq, Eq, Copy, Clone)]
pub enum ContextFrame {
    Unspecified,
    Where,
    GroupBy,
    OrderBy,
    Projection,
}

impl Default for ContextFrame {
    fn default() -> Self {
        Self::Unspecified
    }
}

#[allow(unused_variables)]
pub trait QueryVisitorMut {
    type Inner<'a>: ExprVisitorMut
    where
        Self: 'a;

    fn enter_query_mut(&mut self) -> crate::Result<()> {
        Ok(())
    }

    fn exit_query_mut(&mut self) -> crate::Result<()> {
        Ok(())
    }

    fn enter_from_mut(&mut self, attrs: &mut NodeAttributes, ident: &str) -> crate::Result<()> {
        Ok(())
    }

    fn exit_from_mut(&mut self, attrs: &mut NodeAttributes, ident: &str) -> crate::Result<()> {
        Ok(())
    }

    fn on_source_events_mut(
        &mut self,
        attrs: &mut NodeAttributes,
        ident: &str,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn on_source_subject_mut(
        &mut self,
        attrs: &mut NodeAttributes,
        ident: &str,
        subject: &mut Subject,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn on_source_subquery_mut(
        &mut self,
        attrs: &mut NodeAttributes,
        ident: &str,
    ) -> crate::Result<bool> {
        Ok(true)
    }

    fn exit_source_mut(&mut self, _attrs: &mut NodeAttributes) -> crate::Result<()> {
        Ok(())
    }

    fn enter_where_clause_mut(
        &mut self,
        attrs: &mut NodeAttributes,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_where_clause_mut(
        &mut self,
        attrs: &mut NodeAttributes,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn enter_group_by_mut(&mut self, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn leave_group_by_mut(&mut self, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn enter_order_by_mut(&mut self, order: &mut Order, _expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn leave_order_by_mut(&mut self, order: &mut Order, _expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn enter_projection_mut(&mut self, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn leave_projection_mut(&mut self, expr: &mut Expr) -> crate::Result<()> {
        Ok(())
    }

    fn expr_visitor_mut<'a>(&'a mut self) -> Self::Inner<'a>;
}

#[allow(unused_variables)]
pub trait ExprVisitorMut {
    fn on_literal(&mut self, attrs: &mut NodeAttributes, lit: &mut Literal) -> crate::Result<()> {
        Ok(())
    }

    fn on_var(&mut self, attrs: &mut NodeAttributes, var: &mut Var) -> crate::Result<()> {
        Ok(())
    }

    fn enter_record(
        &mut self,
        attrs: &mut NodeAttributes,
        record: &mut [Expr],
    ) -> crate::Result<()> {
        Ok(())
    }

    fn enter_field(
        &mut self,
        attrs: &mut NodeAttributes,
        label: &mut str,
        value: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_field(
        &mut self,
        attrs: &mut NodeAttributes,
        label: &mut str,
        value: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_record(
        &mut self,
        attrs: &mut NodeAttributes,
        record: &mut [Expr],
    ) -> crate::Result<()> {
        Ok(())
    }

    fn enter_array(
        &mut self,
        attrs: &mut NodeAttributes,
        values: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_array(
        &mut self,
        attrs: &mut NodeAttributes,
        values: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn enter_app(
        &mut self,
        attrs: &mut NodeAttributes,
        name: &str,
        params: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_app(
        &mut self,
        attrs: &mut NodeAttributes,
        name: &str,
        params: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn enter_binary_op(
        &mut self,
        attrs: &mut NodeAttributes,
        op: &Operation,
        lhs: &mut Expr,
        rhs: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_binary_op(
        &mut self,
        attrs: &mut NodeAttributes,
        op: &Operation,
        lhs: &mut Expr,
        rhs: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn enter_unary_op(
        &mut self,
        attrs: &mut NodeAttributes,
        op: &Operation,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn exit_unary_op(
        &mut self,
        attrs: &mut NodeAttributes,
        op: &Operation,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        Ok(())
    }
}

#[allow(unused_variables)]
pub trait QueryVisitor {
    type Inner<'a>: ExprVisitor
    where
        Self: 'a;

    fn enter_query(&mut self, attrs: &NodeAttributes) {}
    fn exit_query(&mut self) {}
    fn enter_from(&mut self, attrs: &NodeAttributes, ident: &str) {}
    fn exit_from(&mut self, attrs: &NodeAttributes, ident: &str) {}
    fn on_source_events(&mut self, attrs: &NodeAttributes, ident: &str) {}
    fn on_source_subject(&mut self, attrs: &NodeAttributes, ident: &str, subject: &Subject) {}
    fn on_source_subquery(&mut self, attrs: &NodeAttributes, ident: &str, query: &Query) -> bool {
        true
    }
    fn exit_source(&mut self, attrs: &NodeAttributes) {}
    fn enter_where_clause(&mut self, attrs: &NodeAttributes, expr: &Expr) {}
    fn exit_where_clause(&mut self, attrs: &NodeAttributes, expr: &Expr) {}
    fn enter_group_by(&mut self, expr: &Expr) {}
    fn leave_group_by(&mut self, expr: &Expr) {}
    fn enter_order_by(&mut self, order: &Order, expr: &Expr) {}
    fn leave_order_by(&mut self, order: &Order, expr: &Expr) {}
    fn enter_projection(&mut self, expr: &Expr) {}
    fn leave_projection(&mut self, expr: &Expr) {}
    fn expr_visitor<'a>(&'a mut self) -> Self::Inner<'a>;
}

#[allow(unused_variables)]
pub trait ExprVisitor {
    fn on_literal(&mut self, attrs: &NodeAttributes, lit: &Literal) {}
    fn on_var(&mut self, attrs: &NodeAttributes, var: &Var) {}
    fn enter_record(&mut self, attrs: &NodeAttributes, record: &[Expr]) {}
    fn enter_field(&mut self, attrs: &NodeAttributes, label: &str, value: &Expr) {}
    fn exit_field(&mut self, attrs: &NodeAttributes, label: &str, value: &Expr) {}
    fn exit_record(&mut self, attrs: &NodeAttributes, record: &[Expr]) {}
    fn enter_array(&mut self, attrs: &NodeAttributes, values: &[Expr]) {}
    fn exit_array(&mut self, attrs: &NodeAttributes, values: &[Expr]) {}
    fn enter_app(&mut self, attrs: &NodeAttributes, name: &str, params: &[Expr]) {}
    fn exit_app(&mut self, attrs: &NodeAttributes, name: &str, params: &[Expr]) {}
    fn enter_binary_op(&mut self, attrs: &NodeAttributes, op: &Operation, lhs: &Expr, rhs: &Expr) {}
    fn exit_binary_op(&mut self, attrs: &NodeAttributes, op: &Operation, lhs: &Expr, rhs: &Expr) {}
    fn enter_unary_op(&mut self, attrs: &NodeAttributes, op: &Operation, expr: &Expr) {}
    fn exit_unary_op(&mut self, attrs: &NodeAttributes, op: &Operation, expr: &Expr) {}
}
