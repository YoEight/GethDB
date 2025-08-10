use crate::{
    Expr, Literal, Operation, Query, Var,
    parser::{ExprVisitor, QueryVisitor},
};

pub enum Instr {
    Push(Literal),
    LoadVar(Var),
    Operation(Operation),
    Array(usize),
    Rec(usize),
    Call(String),
}

pub fn codegen(query: &mut Query) -> crate::Result<Vec<Instr>> {
    let mut state = Codegen::default();

    query.dfs_post_order(&mut state)?;

    Ok(state.instrs)
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
    fn on_literal(
        &mut self,
        _attrs: &mut crate::parser::Attributes,
        lit: &mut Literal,
    ) -> crate::Result<()> {
        self.inner.instrs.push(Instr::Push(lit.clone()));

        Ok(())
    }

    fn on_var(
        &mut self,
        _attrs: &mut crate::parser::Attributes,
        var: &mut Var,
    ) -> crate::Result<()> {
        self.inner.instrs.push(Instr::LoadVar(var.clone()));

        Ok(())
    }

    fn enter_record_entry(
        &mut self,
        _attrs: &mut crate::parser::Attributes,
        key: &str,
        _expr: &mut Expr,
    ) -> crate::Result<()> {
        self.inner
            .instrs
            .push(Instr::Push(Literal::String(key.to_string())));

        Ok(())
    }

    fn exit_record(
        &mut self,
        _attrs: &mut crate::parser::Attributes,
        record: &mut crate::parser::Record,
    ) -> crate::Result<()> {
        self.inner.instrs.push(Instr::Rec(record.fields.len()));

        Ok(())
    }

    fn exit_array(
        &mut self,
        _attrs: &mut crate::parser::Attributes,
        values: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        self.inner.instrs.push(Instr::Array(values.len()));

        Ok(())
    }

    fn exit_app(
        &mut self,
        _attrs: &mut crate::parser::Attributes,
        name: &str,
        _params: &mut Vec<Expr>,
    ) -> crate::Result<()> {
        self.inner.instrs.push(Instr::Call(name.to_string()));

        Ok(())
    }

    fn exit_binary_op(
        &mut self,
        _attrs: &mut crate::parser::Attributes,
        op: &Operation,
        _lhs: &mut Expr,
        _rhs: &mut Expr,
    ) -> crate::Result<()> {
        self.inner.instrs.push(Instr::Operation(*op));

        Ok(())
    }

    fn exit_unary_op(
        &mut self,
        _attrs: &mut crate::parser::Attributes,
        op: &Operation,
        _expr: &mut Expr,
    ) -> crate::Result<()> {
        self.inner.instrs.push(Instr::Operation(*op));

        Ok(())
    }
}
