use crate::{Expr, ExprVisitor, Literal, NodeAttributes, Operation, Query, QueryVisitor, Var};

pub enum Instr {
    Push(Literal),
    LoadVar(Var),
    Operation(Operation),
    Array(usize),
    Rec(usize),
    Call(String),
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

    fn on_var(&mut self, _attrs: &NodeAttributes, var: &Var) {
        self.inner.instrs.push(Instr::LoadVar(var.clone()));
    }

    fn exit_record(&mut self, _attrs: &NodeAttributes, record: &[Expr]) {
        self.inner.instrs.push(Instr::Rec(record.len()));
    }

    fn exit_array(&mut self, _attrs: &NodeAttributes, values: &[Expr]) {
        self.inner.instrs.push(Instr::Array(values.len()));
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
