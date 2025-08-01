use std::collections::HashMap;

use crate::{Instr, Literal, Operation, parser::Record};

pub enum EvalError {
    UnexpectedRuntimeError,
}

pub struct RuntimeEnv {
    inner: HashMap<String, Literal>,
}

pub struct Rec {
    inner: HashMap<String, Literal>,
}

pub enum Output {
    Literal(Literal),
    Record(Rec),
}

fn pop_value(stack: &mut Vec<Instr>) -> Result<Instr, EvalError> {
    if let Some(instr) = stack.pop() {
        return Ok(instr);
    }

    Err(EvalError::UnexpectedRuntimeError)
}

pub fn eval(runtime: RuntimeEnv, mut stack: Vec<Instr>) -> Result<Output, EvalError> {
    while let Some(instr) = stack.pop() {
        match instr {
            Instr::Literal(l) => return Ok(Output::Literal(l)),

            Instr::Operation(op) => match op {
                Operation::And => {
                    let lhs = pop_value(&mut stack)?;
                    let rhs = pop_value(&mut stack)?;

                    match (lhs, rhs) {
                        (Instr::Literal(Literal::Bool(a)), Instr::Literal(Literal::Bool(b))) => {
                            stack.push(Instr::Literal(Literal::Bool(a && b)));
                        }

                        _ => return Err(EvalError::UnexpectedRuntimeError),
                    }
                }

                Operation::Or => {
                    let lhs = pop_value(&mut stack)?;
                    let rhs = pop_value(&mut stack)?;

                    match (lhs, rhs) {
                        (Instr::Literal(Literal::Bool(a)), Instr::Literal(Literal::Bool(b))) => {
                            stack.push(Instr::Literal(Literal::Bool(a || b)));
                        }

                        _ => return Err(EvalError::UnexpectedRuntimeError),
                    }
                }

                Operation::Xor => {
                    let lhs = pop_value(&mut stack)?;
                    let rhs = pop_value(&mut stack)?;

                    match (lhs, rhs) {
                        (Instr::Literal(Literal::Bool(a)), Instr::Literal(Literal::Bool(b))) => {
                            stack.push(Instr::Literal(Literal::Bool(a ^ b)));
                        }

                        _ => return Err(EvalError::UnexpectedRuntimeError),
                    }
                }

                Operation::Not => {
                    let value = pop_value(&mut stack)?;

                    if let Instr::Literal(Literal::Bool(a)) = value {
                        stack.push(Instr::Literal(Literal::Bool(!a)));
                        continue;
                    }

                    return Err(EvalError::UnexpectedRuntimeError);
                }

                Operation::Contains => todo!(),
                Operation::Equal => todo!(),
                Operation::NotEqual => todo!(),
                Operation::LessThan => todo!(),
                Operation::GreaterThan => todo!(),
                Operation::LessThanOrEqual => todo!(),
                Operation::GreaterThanOrEqual => todo!(),
            },

            Instr::Lookup(var) => todo!(),
            Instr::Array(_) => todo!(),
            Instr::Call(_, _) => todo!(),
            Instr::Key(_) => todo!(),
            Instr::Record(_) => todo!(),
        }
    }

    todo!()
}
