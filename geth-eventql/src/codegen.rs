use crate::{
    Expr, ExprVisitor, Literal, NodeAttributes, Operation, Query, QueryVisitor, Subject, Var,
    Where, parser::Record,
};

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

pub enum Instr {
    Push(Literal),
    LoadVar,
    Operation(Operation),
    Array,
    Rec,
    Call,
}

impl Instr {
    pub fn lit(value: impl IntoLiteral) -> Self {
        Instr::Push(value.into_literal())
    }
}

pub fn codegen_where_clause(where_clause: &Where) -> Vec<Instr> {
    let mut state = ExprCodegen::default();

    where_clause.expr.dfs_post_order(&mut state);

    state.instrs
}

#[derive(Default)]
pub struct ExprCodegen {
    instrs: Vec<Instr>,
}

impl ExprVisitor for ExprCodegen {
    fn on_literal(&mut self, _attrs: &NodeAttributes, lit: &Literal) {
        self.instrs.push(Instr::Push(lit.clone()));
    }

    fn on_var(&mut self, _attrs: &NodeAttributes, var: &Var) {
        for seg in var.path.iter().rev() {
            self.instrs.push(Instr::lit(seg));
        }

        self.instrs.push(Instr::lit(var.path.len() + 1));
        self.instrs.push(Instr::Array);
        self.instrs.push(Instr::lit(var.name.as_str()));
        self.instrs.push(Instr::LoadVar);
    }

    fn exit_record(&mut self, _attrs: &NodeAttributes, record: &Record) {
        for key in record.fields.keys() {
            self.instrs.push(Instr::lit(key));
        }

        self.instrs.push(Instr::lit(record.fields.len()));
        self.instrs.push(Instr::Rec);
    }

    fn exit_array(&mut self, _attrs: &NodeAttributes, values: &[Expr]) {
        self.instrs.push(Instr::lit(values.len()));
        self.instrs.push(Instr::Array);
    }

    fn exit_app(&mut self, _attrs: &NodeAttributes, name: &str, _params: &[Expr]) {
        self.instrs.push(Instr::lit(name));
        self.instrs.push(Instr::Call);
    }

    fn exit_binary_op(
        &mut self,
        _attrs: &NodeAttributes,
        op: &Operation,
        _lhs: &Expr,
        _rhs: &Expr,
    ) {
        self.instrs.push(Instr::Operation(*op));
    }

    fn exit_unary_op(&mut self, _attrs: &NodeAttributes, op: &Operation, _expr: &Expr) {
        self.instrs.push(Instr::Operation(*op));
    }
}
