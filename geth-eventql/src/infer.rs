use std::{collections::HashMap, fmt::Display};

use crate::{
    Expr, Literal, Operation, Pos, Query, Scopes, Var,
    error::InferError,
    parser::{ExprVisitorMut, NodeAttributes, QueryVisitorMut, Record},
};

pub struct InferedQuery {
    assumptions: Assumptions,
    scopes: Scopes,
    query: Query,
}

impl InferedQuery {
    pub fn assumptions(&self) -> &Assumptions {
        &self.assumptions
    }

    pub fn scopes(&self) -> &Scopes {
        &self.scopes
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn query_mut(&mut self) -> &mut Query {
        &mut self.query
    }
}

pub struct Infer {
    pos: Pos,
    scope: u64,
}

impl Infer {
    pub fn pos(&self) -> Pos {
        self.pos
    }

    pub fn scope(&self) -> u64 {
        self.scope
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Type {
    Unspecified,
    Integer,
    Float,
    String,
    Bool,
    Array,
    Record,
    Subject,
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Unspecified => write!(f, "<unspecified>"),
            Type::Integer => write!(f, "Integer"),
            Type::Float => write!(f, "Float"),
            Type::String => write!(f, "String"),
            Type::Bool => write!(f, "Bool"),
            Type::Array => write!(f, "Array"),
            Type::Record => write!(f, "Record"),
            Type::Subject => write!(f, "Subject"),
        }
    }
}

impl Type {
    fn project(lit: &Literal) -> Self {
        match lit {
            Literal::String(_) => Type::String,
            Literal::Integral(_) => Type::Integer,
            Literal::Float(_) => Type::Float,
            Literal::Bool(_) => Type::Bool,
            Literal::Subject(_) => Type::Subject,
        }
    }
}

#[derive(Default)]
pub struct Assumptions {
    inner: HashMap<String, Type>,
}

impl Assumptions {
    pub fn lookup_type_info(&self, scope: u64, var: &Var) -> Type {
        let key = urn(scope, &var.name, &var.path);
        if let Some(tpe) = self.inner.get(&key) {
            return *tpe;
        }

        Type::Unspecified
    }
}

pub fn infer(scopes: Scopes, mut query: Query) -> crate::Result<InferedQuery> {
    let mut inner = HashMap::new();

    for scope in scopes.iter() {
        for (name, props) in scope.vars() {
            inner.insert(format!("{}:{name}", scope.id()), Type::Record);
            inner.insert(format!("{}:{name}:specversion", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:id", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:time", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:source", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:subject", scope.id()), Type::Subject);
            inner.insert(format!("{}:{name}:type", scope.id()), Type::String);
            inner.insert(
                format!("{}:{name}:datacontenttype", scope.id()),
                Type::String,
            );
            inner.insert(format!("{}:{name}:data", scope.id()), Type::Record);
            inner.insert(
                format!("{}:{name}:predecessorhash", scope.id()),
                Type::Integer,
            );
            inner.insert(format!("{}:{name}:hash", scope.id()), Type::Integer);

            for prop in props.iter() {
                inner.insert(
                    format!("{}:{name}:data:{prop}", scope.id()),
                    Type::Unspecified,
                );
            }
        }
    }

    let mut type_check = Typecheck {
        assumptions: inner,
        scopes,
    };

    query.dfs_post_order_mut(&mut type_check)?;

    Ok(InferedQuery {
        assumptions: Assumptions {
            inner: type_check.assumptions,
        },
        scopes: type_check.scopes,
        query,
    })
}

struct Typecheck {
    assumptions: HashMap<String, Type>,
    scopes: Scopes,
}

fn urn(scope: u64, name: &String, path: &Vec<String>) -> String {
    let mut agg = format!("{scope}:{name}");

    for prop in path {
        agg.push(':');
        agg.push_str(prop);
    }

    agg
}

impl Typecheck {
    fn lookup_type_info(&self, scope: u64, var: &Var) -> Type {
        let key = urn(scope, &var.name, &var.path);
        if let Some(tpe) = self.assumptions.get(&key) {
            return *tpe;
        }

        Type::Unspecified
    }

    fn set_type_info(&mut self, scope: u64, var: &Var, assumption: Type) {
        let key = urn(scope, &var.name, &var.path);
        self.assumptions.insert(key, assumption);
    }
}

impl QueryVisitorMut for Typecheck {
    type Inner<'a> = TypecheckExpr<'a>;

    fn enter_where_clause(
        &mut self,
        attrs: &mut NodeAttributes,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        attrs.tpe = Type::Bool;
        expr.attrs.tpe = Type::Bool;

        Ok(())
    }

    fn expr_visitor<'a>(&'a mut self) -> Self::Inner<'a> {
        TypecheckExpr { inner: self }
    }
}

struct TypecheckExpr<'a> {
    inner: &'a mut Typecheck,
}

impl ExprVisitorMut for TypecheckExpr<'_> {
    fn on_literal(&mut self, attrs: &mut NodeAttributes, lit: &mut Literal) -> crate::Result<()> {
        let type_proj = Type::project(lit);

        if attrs.tpe != Type::Unspecified && attrs.tpe != type_proj {
            bail!(attrs.pos, InferError::TypeMismatch(attrs.tpe, type_proj));
        }

        attrs.tpe = type_proj;

        Ok(())
    }

    fn on_var(&mut self, attrs: &mut NodeAttributes, var: &mut Var) -> crate::Result<()> {
        let register_assumption = self.inner.lookup_type_info(attrs.scope, var);

        if attrs.tpe == Type::Unspecified && register_assumption == attrs.tpe {
            attrs.tpe = Type::Unspecified;
        } else if register_assumption == Type::Unspecified {
            self.inner.set_type_info(attrs.scope, var, attrs.tpe);
        } else if attrs.tpe == Type::Unspecified {
            attrs.tpe = register_assumption;
        } else if attrs.tpe != register_assumption {
            bail!(
                attrs.pos,
                InferError::VarTypeMismatch(var.clone(), attrs.tpe, register_assumption)
            );
        }

        Ok(())
    }

    fn exit_record(
        &mut self,
        attrs: &mut NodeAttributes,
        _record: &mut Record,
    ) -> crate::Result<()> {
        if attrs.tpe != Type::Unspecified && attrs.tpe != Type::Record {
            bail!(attrs.pos, InferError::TypeMismatch(attrs.tpe, Type::Record));
        }

        attrs.tpe = Type::Record;

        Ok(())
    }

    fn exit_array(
        &mut self,
        attrs: &mut NodeAttributes,
        _values: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        if attrs.tpe != Type::Unspecified && attrs.tpe != Type::Array {
            bail!(attrs.pos, InferError::TypeMismatch(attrs.tpe, Type::Array));
        }

        attrs.tpe = Type::Array;

        Ok(())
    }

    fn exit_app(
        &mut self,
        _attrs: &mut NodeAttributes,
        _name: &str,
        _params: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        // TODO - we can make a lot of assumptions when it comes to the return type of the
        // function call.
        //
        // TODO - based on the function we call, we can also make assumption about the type of its
        // parameters. Right now we are just going to ignore it.

        Ok(())
    }

    fn enter_binary_op(
        &mut self,
        attrs: &mut NodeAttributes,
        op: &Operation,
        lhs: &mut Expr,
        rhs: &mut Expr,
    ) -> crate::Result<()> {
        match op {
            Operation::And | Operation::Or | Operation::Xor => {
                lhs.attrs.tpe = Type::Bool;
                rhs.attrs.tpe = Type::Bool;
            }

            Operation::Contains => {
                lhs.attrs.tpe = Type::Array;
            }

            _ => {}
        }

        // TODO - to change when we are going to angle arithmetic operation
        attrs.tpe = Type::Bool;

        Ok(())
    }

    fn exit_binary_op(
        &mut self,
        attrs: &mut NodeAttributes,
        op: &Operation,
        lhs: &mut Expr,
        rhs: &mut Expr,
    ) -> crate::Result<()> {
        let result_type = match op {
            Operation::And
            | Operation::Or
            | Operation::Xor
            | Operation::Contains
            | Operation::Equal
            | Operation::NotEqual
            | Operation::LessThan
            | Operation::GreaterThan
            | Operation::LessThanOrEqual
            | Operation::GreaterThanOrEqual => Type::Bool,

            Operation::Not => {
                bail!(attrs.pos, InferError::UnsupportedBinaryOperation(*op));
            }
        };

        if attrs.tpe != Type::Unspecified && attrs.tpe != result_type {
            bail!(attrs.pos, InferError::TypeMismatch(attrs.tpe, result_type));
        }

        if lhs.attrs.tpe == Type::Unspecified
            && rhs.attrs.tpe != Type::Unspecified
            && operation_requires_same_type(op)
        {
            lhs.attrs.tpe = rhs.attrs.tpe;

            if let Some(var) = lhs.as_var() {
                self.inner
                    .set_type_info(rhs.attrs.scope, var, rhs.attrs.tpe);
            }
        } else if rhs.attrs.tpe == Type::Unspecified
            && lhs.attrs.tpe != Type::Unspecified
            && operation_requires_same_type(op)
        {
            rhs.attrs.tpe = lhs.attrs.tpe;

            if let Some(var) = rhs.as_var() {
                self.inner
                    .set_type_info(lhs.attrs.scope, var, lhs.attrs.tpe);
            }
        }

        if operation_requires_same_type(op) && lhs.attrs.tpe != rhs.attrs.tpe {
            bail!(
                attrs.pos,
                InferError::TypeMismatch(lhs.attrs.tpe, rhs.attrs.tpe)
            );
        }

        if op == &Operation::Contains && lhs.attrs.tpe != Type::Array {
            bail!(
                attrs.pos,
                InferError::TypeMismatch(rhs.attrs.tpe, Type::Array)
            );
        }

        Ok(())
    }

    fn enter_unary_op(
        &mut self,
        attrs: &mut NodeAttributes,
        op: &Operation,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        if let Operation::Not = op {
            attrs.tpe = Type::Bool;
            expr.attrs.tpe = Type::Bool;
        }

        Ok(())
    }

    fn exit_unary_op(
        &mut self,
        attrs: &mut NodeAttributes,
        op: &Operation,
        expr: &mut Expr,
    ) -> crate::Result<()> {
        let result_type = if op == &Operation::Not {
            Type::Bool
        } else {
            bail!(
                expr.attrs.pos,
                InferError::TypeMismatch(attrs.tpe, Type::Bool)
            );
        };

        if attrs.tpe != Type::Unspecified && attrs.tpe != result_type {
            bail!(
                expr.attrs.pos,
                InferError::TypeMismatch(attrs.tpe, result_type)
            );
        }

        if expr.attrs.tpe != Type::Unspecified && expr.attrs.tpe != result_type {
            bail!(
                expr.attrs.pos,
                InferError::TypeMismatch(attrs.tpe, result_type)
            );
        }

        Ok(())
    }
}

fn operation_requires_same_type(op: &Operation) -> bool {
    !matches!(op, Operation::Contains)
}
