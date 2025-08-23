use crate::{Expr, ExprVisitor, Literal, NodeAttributes, Operation, Var, Where};

pub trait IntoLiteral {
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

impl IntoLiteral for Literal {
    fn into_literal(self) -> Literal {
        self
    }
}

pub enum Instr {
    Push(Literal),
    LoadVar(Var),
    Operation(Operation),
    Array(usize),
    Rec(usize),
    Call(String),
}

impl Instr {
    fn lit(value: impl IntoLiteral) -> Self {
        Instr::Push(value.into_literal())
    }
}

pub fn codegen_where_clause(where_clause: &Where) -> Vec<Instr> {
    let mut state = ExprCodegen::default();

    where_clause.expr.dfs_post_order(&mut state);

    state.emit.inner
}

#[derive(Default)]
struct Emit {
    inner: Vec<Instr>,
}

impl Emit {
    fn push<L>(&mut self, lit: L)
    where
        L: IntoLiteral,
    {
        self.inner.push(Instr::lit(lit));
    }

    fn load(&mut self, var: Var) {
        self.inner.push(Instr::LoadVar(var.clone()));
    }

    fn rec(&mut self, siz: usize) {
        self.inner.push(Instr::Rec(siz));
    }

    fn array(&mut self, siz: usize) {
        self.inner.push(Instr::Array(siz));
    }

    fn call(&mut self, name: &str) {
        self.inner.push(Instr::Call(name.to_string()));
    }

    fn op(&mut self, op: Operation) {
        self.inner.push(Instr::Operation(op));
    }
}

#[derive(Default)]
pub struct ExprCodegen {
    emit: Emit,
}

impl ExprVisitor for ExprCodegen {
    fn on_literal(&mut self, _attrs: &NodeAttributes, lit: &Literal) {
        self.emit.push(lit.clone());
    }

    fn on_var(&mut self, _attrs: &NodeAttributes, var: &Var) {
        self.emit.load(var.clone());
    }

    fn exit_array(&mut self, _attrs: &NodeAttributes, values: &[Expr]) {
        self.emit.array(values.len());
    }

    fn exit_field(&mut self, _attrs: &NodeAttributes, label: &str, _value: &Expr) {
        self.emit.push(label);
    }

    fn exit_record(&mut self, _attrs: &NodeAttributes, record: &[Expr]) {
        self.emit.rec(record.len());
    }

    fn exit_app(&mut self, _attrs: &NodeAttributes, name: &str, _params: &[Expr]) {
        self.emit.call(name);
    }

    fn exit_binary_op(
        &mut self,
        _attrs: &NodeAttributes,
        op: &Operation,
        _lhs: &Expr,
        _rhs: &Expr,
    ) {
        self.emit.op(*op);
    }

    fn exit_unary_op(&mut self, _attrs: &NodeAttributes, op: &Operation, _expr: &Expr) {
        self.emit.op(*op);
    }
}
