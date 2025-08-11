use std::collections::HashMap;

use crate::{Instr, Literal, Operation, Var};

pub enum EvalError {
    UnexpectedRuntimeError,
    UnexpectedVarNotFoundError(Var),
}

pub struct Dictionary {
    pub inner: HashMap<String, Literal>,
}

impl Dictionary {
    fn lookup(&self, _var: &Var) -> Result<Literal> {
        todo!()
    }
}

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

type Result<A> = std::result::Result<A, EvalError>;

pub struct Rec {
    pub fields: HashMap<String, Entry>,
}

pub enum Entry {
    Literal(Literal),
    Array(Vec<Entry>),
    Record(Rec),
}

#[derive(Default)]
pub struct Stack {
    inner: Vec<Entry>,
}

impl Stack {
    fn pop(&mut self) -> Option<Entry> {
        self.inner.pop()
    }

    fn pop_or_bail(&mut self) -> Result<Entry> {
        if let Some(item) = self.pop() {
            return Ok(item);
        }

        Err(EvalError::UnexpectedRuntimeError)
    }

    fn pop_as_literal_or_bail(&mut self) -> Result<Literal> {
        match self.pop_or_bail()? {
            Entry::Literal(lit) => Ok(lit),
            _ => Err(EvalError::UnexpectedRuntimeError),
        }
    }

    fn pop_as_string_or_bail(&mut self) -> Result<String> {
        match self.pop_as_literal_or_bail()? {
            Literal::String(s) => Ok(s),
            _ => Err(EvalError::UnexpectedRuntimeError),
        }
    }

    fn pop_as_number_or_bail(&mut self) -> Result<Either<i64, f64>> {
        match self.pop_as_literal_or_bail()? {
            Literal::Integral(i) => Ok(Either::Left(i)),
            Literal::Float(f) => Ok(Either::Right(f)),
            _ => Err(EvalError::UnexpectedRuntimeError),
        }
    }

    fn pop_as_bool_or_bail(&mut self) -> Result<bool> {
        match self.pop_as_literal_or_bail()? {
            Literal::Bool(b) => Ok(b),
            _ => Err(EvalError::UnexpectedRuntimeError),
        }
    }

    fn pop_as_array_or_bail(&mut self) -> Result<Vec<Entry>> {
        if let Entry::Array(xs) = self.pop_or_bail()? {
            return Ok(xs);
        }

        Err(EvalError::UnexpectedRuntimeError)
    }

    fn push_literal(&mut self, lit: Literal) {
        self.inner.push(Entry::Literal(lit));
    }

    fn push_array(&mut self, array: Vec<Entry>) {
        self.inner.push(Entry::Array(array));
    }

    fn push_record(&mut self, rec: Rec) {
        self.inner.push(Entry::Record(rec));
    }
}

pub fn eval(dict: &Dictionary, instrs: Vec<Instr>) -> Result<Option<Entry>> {
    let mut stack = Stack::default();

    for instr in instrs {
        match instr {
            Instr::Push(lit) => stack.push_literal(lit),

            Instr::LoadVar(var) => {
                let lit = dict.lookup(&var)?;
                stack.push_literal(lit);
            }

            Instr::Operation(op) => match op {
                Operation::And => {
                    let rhs = stack.pop_as_bool_or_bail()?;
                    let lhs = stack.pop_as_bool_or_bail()?;

                    stack.push_literal(Literal::Bool(lhs && rhs));
                }

                Operation::Or => {
                    let rhs = stack.pop_as_bool_or_bail()?;
                    let lhs = stack.pop_as_bool_or_bail()?;

                    stack.push_literal(Literal::Bool(lhs || rhs));
                }

                Operation::Xor => {
                    let rhs = stack.pop_as_bool_or_bail()?;
                    let lhs = stack.pop_as_bool_or_bail()?;

                    stack.push_literal(Literal::Bool(lhs ^ rhs));
                }

                Operation::Not => {
                    let value = stack.pop_as_bool_or_bail()?;

                    stack.push_literal(Literal::Bool(!value));
                }

                Operation::Contains => {
                    let array = stack.pop_as_array_or_bail()?;
                    let value = stack.pop_or_bail()?;

                    for elem in array {
                        match (&value, elem) {
                            (
                                Entry::Literal(Literal::Integral(value)),
                                Entry::Literal(Literal::Integral(elem)),
                            ) if *value == elem => {
                                stack.push_literal(Literal::Bool(true));
                                continue;
                            }

                            (
                                Entry::Literal(Literal::Float(value)),
                                Entry::Literal(Literal::Float(elem)),
                            ) if *value == elem => {
                                stack.push_literal(Literal::Bool(true));
                                continue;
                            }

                            (
                                Entry::Literal(Literal::Bool(value)),
                                Entry::Literal(Literal::Bool(elem)),
                            ) if *value == elem => {
                                stack.push_literal(Literal::Bool(true));
                                continue;
                            }

                            (
                                Entry::Literal(Literal::String(value)),
                                Entry::Literal(Literal::String(elem)),
                            ) if value == &elem => {
                                stack.push_literal(Literal::Bool(true));
                                continue;
                            }

                            (
                                Entry::Literal(Literal::Subject(value)),
                                Entry::Literal(Literal::Subject(elem)),
                            ) if value == &elem => {
                                stack.push_literal(Literal::Bool(true));
                                continue;
                            }

                            _ => {}
                        }
                    }

                    stack.push_literal(Literal::Bool(false));
                }

                Operation::Equal => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Entry::Literal(Literal::Integral(lhs)),
                            Entry::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        (
                            Entry::Literal(Literal::Float(lhs)),
                            Entry::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        (
                            Entry::Literal(Literal::String(lhs)),
                            Entry::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        (
                            Entry::Literal(Literal::Bool(lhs)),
                            Entry::Literal(Literal::Bool(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        (
                            Entry::Literal(Literal::Subject(lhs)),
                            Entry::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        _ => stack.push_literal(Literal::Bool(false)),
                    }
                }

                Operation::NotEqual => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Entry::Literal(Literal::Integral(lhs)),
                            Entry::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        (
                            Entry::Literal(Literal::Float(lhs)),
                            Entry::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        (
                            Entry::Literal(Literal::String(lhs)),
                            Entry::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        (
                            Entry::Literal(Literal::Bool(lhs)),
                            Entry::Literal(Literal::Bool(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        (
                            Entry::Literal(Literal::Subject(lhs)),
                            Entry::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        _ => stack.push_literal(Literal::Bool(false)),
                    }
                }

                Operation::LessThan => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Entry::Literal(Literal::Integral(lhs)),
                            Entry::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs < rhs));
                        }

                        (
                            Entry::Literal(Literal::Float(lhs)),
                            Entry::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs < rhs));
                        }

                        (
                            Entry::Literal(Literal::String(lhs)),
                            Entry::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs < rhs));
                        }

                        (
                            Entry::Literal(Literal::Bool(lhs)),
                            Entry::Literal(Literal::Bool(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(!lhs & rhs));
                        }

                        (
                            Entry::Literal(Literal::Subject(lhs)),
                            Entry::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs < rhs));
                        }

                        _ => stack.push_literal(Literal::Bool(false)),
                    }
                }

                Operation::GreaterThan => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Entry::Literal(Literal::Integral(lhs)),
                            Entry::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs > rhs));
                        }

                        (
                            Entry::Literal(Literal::Float(lhs)),
                            Entry::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs > rhs));
                        }

                        (
                            Entry::Literal(Literal::String(lhs)),
                            Entry::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs > rhs));
                        }

                        (
                            Entry::Literal(Literal::Bool(lhs)),
                            Entry::Literal(Literal::Bool(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs & !rhs));
                        }

                        (
                            Entry::Literal(Literal::Subject(lhs)),
                            Entry::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs > rhs));
                        }

                        _ => stack.push_literal(Literal::Bool(false)),
                    }
                }

                Operation::LessThanOrEqual => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Entry::Literal(Literal::Integral(lhs)),
                            Entry::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        (
                            Entry::Literal(Literal::Float(lhs)),
                            Entry::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        (
                            Entry::Literal(Literal::String(lhs)),
                            Entry::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        (
                            Entry::Literal(Literal::Bool(lhs)),
                            Entry::Literal(Literal::Bool(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        (
                            Entry::Literal(Literal::Subject(lhs)),
                            Entry::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        _ => stack.push_literal(Literal::Bool(false)),
                    }
                }

                Operation::GreaterThanOrEqual => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Entry::Literal(Literal::Integral(lhs)),
                            Entry::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        (
                            Entry::Literal(Literal::Float(lhs)),
                            Entry::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        (
                            Entry::Literal(Literal::String(lhs)),
                            Entry::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        (
                            Entry::Literal(Literal::Bool(lhs)),
                            Entry::Literal(Literal::Bool(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        (
                            Entry::Literal(Literal::Subject(lhs)),
                            Entry::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        _ => stack.push_literal(Literal::Bool(false)),
                    }
                }
            },

            Instr::Array(siz) => {
                let mut array = Vec::with_capacity(siz);

                for _ in 0..siz {
                    array.push(stack.pop_or_bail()?);
                }

                stack.push_array(array);
            }

            Instr::Rec(siz) => {
                let mut fields = HashMap::with_capacity(siz);

                for _ in 0..siz {
                    let value = stack.pop_or_bail()?;
                    let key = stack.pop_as_string_or_bail()?;

                    fields.insert(key, value);
                }

                stack.push_record(Rec { fields });
            }

            Instr::Call(fun_name) => match fun_name.as_str() {
                "abs" => match stack.pop_as_number_or_bail()? {
                    Either::Left(i) => stack.push_literal(Literal::Integral(i.abs())),
                    Either::Right(f) => stack.push_literal(Literal::Float(f.abs())),
                },

                "ceil" => match stack.pop_as_number_or_bail()? {
                    Either::Left(i) => stack.push_literal(Literal::Integral(i)),
                    Either::Right(f) => stack.push_literal(Literal::Float(f.ceil())),
                },

                "floor" => match stack.pop_as_number_or_bail()? {
                    Either::Left(i) => stack.push_literal(Literal::Integral(i)),
                    Either::Right(f) => stack.push_literal(Literal::Float(f.floor())),
                },

                "sin" => match stack.pop_as_number_or_bail()? {
                    Either::Left(i) => stack.push_literal(Literal::Float((i as f64).sin())),
                    Either::Right(f) => stack.push_literal(Literal::Float(f.sin())),
                },

                "cos" => match stack.pop_as_number_or_bail()? {
                    Either::Left(i) => stack.push_literal(Literal::Float((i as f64).cos())),
                    Either::Right(f) => stack.push_literal(Literal::Float(f.cos())),
                },

                "tan" => match stack.pop_as_number_or_bail()? {
                    Either::Left(i) => stack.push_literal(Literal::Float((i as f64).tan())),
                    Either::Right(f) => stack.push_literal(Literal::Float(f.tan())),
                },

                _ => return Err(EvalError::UnexpectedRuntimeError),
            },
        }
    }

    Ok(stack.pop())
}
