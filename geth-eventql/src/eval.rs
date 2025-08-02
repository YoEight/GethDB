use std::collections::HashMap;

use crate::{Instr, Literal, Operation, Var};

pub enum EvalError {
    UnexpectedRuntimeError,
    UnexpectedVarNotFoundError(Var),
}

pub struct Dictionary {
    inner: HashMap<String, Literal>,
}

impl Dictionary {
    fn lookup(&self, var: &Var) -> Result<Literal> {
        todo!()
    }
}

pub struct Rec {
    inner: HashMap<String, Literal>,
}

pub enum Output {
    Literal(Literal),
    Record(Rec),
}

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

type Result<A> = std::result::Result<A, EvalError>;

pub struct Stack {
    inner: Vec<Instr>,
}

impl Stack {
    fn pop_or_bail(&mut self) -> Result<Instr> {
        if let Some(instr) = self.inner.pop() {
            return Ok(instr);
        }

        Err(EvalError::UnexpectedRuntimeError)
    }

    fn pop_as_literal_or_bail(&mut self, dict: &Dictionary) -> Result<Literal> {
        todo!()
    }

    fn pop_as_string_or_bail(&mut self, dict: &Dictionary) -> Result<String> {
        match self.pop_or_bail()? {
            Instr::Literal(Literal::String(s)) => Ok(s),

            Instr::Lookup(var) => {
                if let Literal::String(s) = dict.lookup(&var)? {
                    return Ok(s);
                }

                Err(EvalError::UnexpectedRuntimeError)
            }

            _ => Err(EvalError::UnexpectedRuntimeError),
        }
    }

    fn pop_as_number_or_bail(&mut self, dict: &Dictionary) -> Result<Either<i64, f64>> {
        match self.pop_or_bail()? {
            Instr::Literal(lit) => match lit {
                Literal::Integral(i) => Ok(Either::Left(i)),
                Literal::Float(f) => Ok(Either::Right(f)),
                _ => Err(EvalError::UnexpectedRuntimeError),
            },

            Instr::Lookup(var) => match dict.lookup(&var)? {
                Literal::Integral(i) => Ok(Either::Left(i)),
                Literal::Float(f) => Ok(Either::Right(f)),
                _ => Err(EvalError::UnexpectedRuntimeError),
            },

            _ => Err(EvalError::UnexpectedRuntimeError),
        }
    }

    fn pop_as_bool_or_bail(&mut self, dict: &Dictionary) -> Result<bool> {
        match self.pop_or_bail()? {
            Instr::Literal(lit) => match lit {
                Literal::Bool(b) => Ok(b),
                _ => Err(EvalError::UnexpectedRuntimeError),
            },

            Instr::Lookup(var) => match dict.lookup(&var)? {
                Literal::Bool(b) => Ok(b),
                _ => Err(EvalError::UnexpectedRuntimeError),
            },

            _ => Err(EvalError::UnexpectedRuntimeError),
        }
    }

    fn pop(&mut self) -> Option<Instr> {
        self.inner.pop()
    }

    fn push(&mut self, instr: Instr) {
        self.inner.push(instr);
    }

    fn execute_builtin_fun(&mut self, dict: &Dictionary, name: &str) -> Result<Literal> {
        let value = match name {
            "lower" => todo!(),
            _ => return Err(EvalError::UnexpectedRuntimeError),
        };

        Ok(value)
    }
}

pub fn eval(dict: &Dictionary, mut stack: Stack) -> Result<Output> {
    while let Some(instr) = stack.pop() {
        match instr {
            Instr::Literal(l) => return Ok(Output::Literal(l)),

            Instr::Operation(op) => match op {
                Operation::And => {
                    let lhs = stack.pop_as_bool_or_bail(dict)?;
                    let rhs = stack.pop_as_bool_or_bail(dict)?;

                    stack.push(Instr::Literal(Literal::Bool(lhs && rhs)));
                }

                Operation::Or => {
                    let lhs = stack.pop_as_bool_or_bail(dict)?;
                    let rhs = stack.pop_as_bool_or_bail(dict)?;

                    stack.push(Instr::Literal(Literal::Bool(lhs || rhs)));
                }

                Operation::Xor => {
                    let lhs = stack.pop_as_bool_or_bail(dict)?;
                    let rhs = stack.pop_as_bool_or_bail(dict)?;

                    stack.push(Instr::Literal(Literal::Bool(lhs ^ rhs)));
                }

                Operation::Not => {
                    let value = stack.pop_as_bool_or_bail(dict)?;

                    stack.push(Instr::Literal(Literal::Bool(!value)));
                }

                Operation::Contains => {
                    let lhs = stack.pop_or_bail()?;

                    let lhs = match lhs {
                        Instr::Literal(lit) => lit,
                        Instr::Lookup(var) => dict.lookup(&var)?,
                        Instr::Call(fun) => todo!(),
                        Instr::Key(_) => todo!(),
                        Instr::Record(_) => todo!(),
                    };

                    let rhs = stack.pop_or_bail()?;
                }

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
