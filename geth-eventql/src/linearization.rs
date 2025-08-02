use crate::{Expr, Infer, Literal, Operation, Value, Var};

pub enum Instr {
    Literal(Literal),
    Operation(Operation),
    Lookup(Var),
    Array(usize),
    Call(String),
    Key(String),
    Record(usize),
}

pub enum Lexem {
    Value(Value<Infer>),
    Array(usize),
    Operation(Operation),
    Call(String),
    Record(usize),
    Key(String),
}

pub fn linearize(expr: Expr<Infer>) -> Vec<Instr> {
    let mut instrs = Vec::new();
    let mut stack = vec![Lexem::Value(expr.value)];

    while let Some(expr) = stack.pop() {
        match expr {
            Lexem::Value(value) => match value {
                Value::Literal(l) => instrs.push(Instr::Literal(l)),
                Value::Var(var) => instrs.push(Instr::Lookup(var)),

                Value::Array(exprs) => {
                    stack.push(Lexem::Array(exprs.len()));

                    for expr in exprs {
                        stack.push(Lexem::Value(expr.value));
                    }
                }

                Value::App { fun, params } => {
                    stack.push(Lexem::Call(fun));

                    for param in params {
                        stack.push(Lexem::Value(param.value));
                    }
                }

                Value::Record(record) => {
                    stack.push(Lexem::Record(record.fields.len()));

                    for (key, expr) in record.fields {
                        stack.push(Lexem::Key(key));
                        stack.push(Lexem::Value(expr.value));
                    }
                }

                Value::Binary { lhs, op, rhs } => {
                    stack.push(Lexem::Operation(op));
                    stack.push(Lexem::Value(lhs.value));
                    stack.push(Lexem::Value(rhs.value));
                }

                Value::Unary { op, expr } => {
                    stack.push(Lexem::Operation(op));
                    stack.push(Lexem::Value(expr.value));
                }
            },

            Lexem::Array(siz) => instrs.push(Instr::Array(siz)),

            Lexem::Operation(op) => instrs.push(Instr::Operation(op)),

            Lexem::Call(fun) => instrs.push(Instr::Call(fun)),

            Lexem::Record(siz) => instrs.push(Instr::Record(siz)),

            Lexem::Key(s) => instrs.push(Instr::Key(s)),
        }
    }

    instrs
}
