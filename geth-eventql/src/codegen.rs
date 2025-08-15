use crate::{
    Expr, ExprVisitor, Literal, NodeAttributes, Operation, Query, QueryVisitor, Subject, Var,
};

trait IntoLiteral {
    fn into_literal(self) -> Literal;
}

impl IntoLiteral for String {
    fn into_literal(self) -> Literal {
        Literal::String(self)
    }
}

impl IntoLiteral for &'_ str {
    fn into_literal(self) -> Literal {
        Literal::String(self.to_string())
    }
}

impl IntoLiteral for &'_ String {
    fn into_literal(self) -> Literal {
        Literal::String(self.to_string())
    }
}

impl IntoLiteral for u64 {
    fn into_literal(self) -> Literal {
        Literal::Integral(self as i64)
    }
}

impl IntoLiteral for i64 {
    fn into_literal(self) -> Literal {
        Literal::Integral(self)
    }
}

impl IntoLiteral for usize {
    fn into_literal(self) -> Literal {
        Literal::Integral(self as i64)
    }
}

pub enum Instr {
    EnterScope,
    ExitScope,
    Push(Literal),
    EnterSource,
    ExitSource,
    LoadVar,
    LoadSource,
    Operation(Operation),
    Array,
    Rec(usize),
    Call(String),
}

impl Instr {
    pub fn push(value: impl IntoLiteral) -> Self {
        Instr::Push(value.into_literal())
    }
}

pub enum SourceKind {
    Events,
    Subject,
    Subquery,
}

pub struct LoadSource {
    binding: String,
    scope: u64,
}

pub fn codegen(query: &Query) -> Vec<Instr> {
    let mut state = Codegen::default();

    query.dfs_post_order(&mut state);

    state.instrs
}

#[derive(Default)]
pub struct Codegen {
    instrs: Vec<Instr>,
}

impl QueryVisitor for Codegen {
    type Inner<'a> = ExprCodegen<'a>;

    fn enter_query(&mut self, attrs: &NodeAttributes) {
        self.instrs.push(Instr::push(attrs.scope));
        self.instrs.push(Instr::EnterScope);
    }

    fn enter_from(&mut self, attrs: &NodeAttributes, _ident: &str) {
        self.instrs.push(Instr::push(attrs.scope));
        self.instrs.push(Instr::EnterSource);
    }

    fn on_source_events(&mut self, attrs: &NodeAttributes, ident: &str) {
        self.instrs.push(Instr::push(attrs.scope));
        self.instrs.push(Instr::push(ident));
        self.instrs.push(Instr::push(0i64));
        self.instrs.push(Instr::LoadSource);
    }

    fn on_source_subject(&mut self, attrs: &NodeAttributes, ident: &str, subject: &Subject) {
        self.instrs.push(Instr::push(attrs.scope));
        self.instrs.push(Instr::push(ident));
        for seg in subject.inner.iter() {
            self.instrs.push(Instr::push(seg));
        }

        self.instrs.push(Instr::push(subject.inner.len()));
        self.instrs.push(Instr::Array);
        self.instrs.push(Instr::push(1i64));
        self.instrs.push(Instr::LoadSource);
    }

    fn on_source_subquery(&mut self, attrs: &NodeAttributes, ident: &str) -> bool {
        self.instrs.push(Instr::push(attrs.scope));
        self.instrs.push(Instr::push(ident));
        self.instrs.push(Instr::push(2i64));
        self.instrs.push(Instr::LoadSource);

        true
    }

    fn exit_from(&mut self, attrs: &NodeAttributes, ident: &str) {
        self.instrs.push(Instr::push(attrs.scope));
        self.instrs.push(Instr::ExitSource);
    }

    fn exit_query(&mut self) {
        self.instrs.push(Instr::ExitScope);
    }

    fn expr_visitor<'a>(&'a mut self) -> Self::Inner<'a> {
        ExprCodegen { inner: self }
    }
}

pub struct ExprCodegen<'a> {
    inner: &'a mut Codegen,
}

impl ExprVisitor for ExprCodegen<'_> {
    fn on_literal(&mut self, _attrs: &NodeAttributes, lit: &Literal) {
        self.inner.instrs.push(Instr::Push(lit.clone()));
    }

    fn on_var(&mut self, attrs: &NodeAttributes, var: &Var) {
        self.inner.instrs.push(Instr::push(attrs.scope));

        self.inner.instrs.push(Instr::push(var.name.as_str()));
        for seg in &var.path {
            self.inner.instrs.push(Instr::push(seg));
        }

        self.inner.instrs.push(Instr::push(var.path.len() + 1));
        self.inner.instrs.push(Instr::Array);
        self.inner.instrs.push(Instr::LoadVar);
    }

    fn exit_record(&mut self, _attrs: &NodeAttributes, record: &[Expr]) {
        self.inner.instrs.push(Instr::Rec(record.len()));
    }

    fn exit_array(&mut self, _attrs: &NodeAttributes, values: &[Expr]) {
        self.inner.instrs.push(Instr::push(values.len()));
        self.inner.instrs.push(Instr::Array);
    }

    fn exit_app(&mut self, _attrs: &NodeAttributes, name: &str, _params: &[Expr]) {
        self.inner.instrs.push(Instr::Call(name.to_string()));
    }

    fn exit_binary_op(
        &mut self,
        _attrs: &NodeAttributes,
        op: &Operation,
        _lhs: &Expr,
        _rhs: &Expr,
    ) {
        self.inner.instrs.push(Instr::Operation(*op));
    }

    fn exit_unary_op(&mut self, _attrs: &NodeAttributes, op: &Operation, _expr: &Expr) {
        self.inner.instrs.push(Instr::Operation(*op));
    }
}
