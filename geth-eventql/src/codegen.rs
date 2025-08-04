use std::mem;

use crate::{Expr, Infer, Literal, Operation, Value, Var};

pub enum Instr {
    Push(Literal),
    LoadVar(Var),
    Operation(Operation),
    Array(usize),
    Rec(usize),
    Call(String),
}

struct Item {
    value: Value<Infer>,
    visited: bool,
    size: usize,
}

impl Item {
    fn new(value: Value<Infer>) -> Self {
        Self {
            value,
            visited: false,
            size: 0,
        }
    }
}

pub fn codegen_expr(expr: Expr<Infer>) -> Vec<Instr> {
    let mut instrs = Vec::new();
    let mut stack = vec![Item::new(expr.value)];

    while let Some(mut item) = stack.pop() {
        match item.value {
            Value::Literal(lit) => instrs.push(Instr::Push(lit)),
            Value::Var(var) => instrs.push(Instr::LoadVar(var)),

            Value::Record(ref mut record) => {
                if item.visited {
                    instrs.push(Instr::Rec(item.size));
                    continue;
                }

                if record.fields.is_empty() {
                    instrs.push(Instr::Rec(0));
                    continue;
                }

                item.visited = true;
                item.size = record.fields.len();
                let fields = mem::take(&mut record.fields);
                stack.push(item);

                for (key, expr) in fields {
                    stack.push(Item::new(expr.value));
                    stack.push(Item::new(Value::Literal(Literal::String(key))));
                }
            }

            Value::Array(ref mut exprs) => {
                if item.visited {
                    instrs.push(Instr::Array(item.size));
                    continue;
                }

                if exprs.is_empty() {
                    instrs.push(Instr::Array(0));
                    continue;
                }

                item.visited = true;
                item.size = exprs.len();
                let elems = mem::take(exprs);
                stack.push(item);

                for elem in elems {
                    stack.push(Item::new(elem.value));
                }
            }

            Value::App {
                ref mut fun,
                ref mut params,
            } => {
                if item.visited || params.is_empty() {
                    instrs.push(Instr::Call(mem::take(fun)));
                    continue;
                }

                item.visited = true;
                let args = mem::take(params);
                stack.push(item);

                for param in args.into_iter().rev() {
                    stack.push(Item::new(param.value));
                }
            }

            Value::Binary {
                ref mut lhs,
                op,
                ref mut rhs,
            } => {
                if item.visited {
                    instrs.push(Instr::Operation(op));
                    continue;
                }

                item.visited = true;

                let tmp_lhs = mem::replace(lhs, unsafe {
                    Box::<Expr<Infer>>::new_uninit().assume_init()
                });

                let tmp_rhs = mem::replace(rhs, unsafe {
                    Box::<Expr<Infer>>::new_uninit().assume_init()
                });

                stack.push(item);
                stack.push(Item::new(tmp_rhs.value));
                stack.push(Item::new(tmp_lhs.value));
            }

            Value::Unary { op, ref mut expr } => {
                if item.visited {
                    instrs.push(Instr::Operation(op));
                    continue;
                }

                item.visited = true;
                let tmp_expr = mem::replace(expr, unsafe { Box::new_uninit().assume_init() });
                stack.push(item);

                stack.push(Item::new(tmp_expr.value));
                todo!()
            }
        }
    }

    instrs
}
