use std::collections::HashMap;

use geth_eventql::{Instr, IntoLiteral, Literal, Operation, Subject, Var};

pub struct Mapping {
    spec_revision: String,
    id: String,
    time: String,
    source: String,
    subject: Subject,
    r#type: String,
    data_content_type: String,
    data: Option<serde_json::Value>,
    predecessorhash: String,
    hash: String,
    cache: HashMap<String, Literal>,
}

#[derive(Default)]
struct Scope {
    inner: HashMap<String, Mapping>,
}

#[derive(Default)]
pub struct Dictionary {
    inner: HashMap<u64, Scope>,
}

impl Dictionary {
    pub fn insert(&mut self, scope: u64, name: &str, m: Mapping) {
        self.inner
            .entry(scope)
            .or_default()
            .inner
            .insert(name.to_string(), m);
    }

    pub fn lookup(&mut self, scope: u64, var: &Var, stack: &mut Stack) {
        let mapping = if let Some(m) = self
            .inner
            .get_mut(&scope)
            .and_then(|s| s.inner.get_mut(var.name.as_str()))
        {
            m
        } else {
            return;
        };

        let mut path_iter = var.path.iter();

        let path = if let Some(p) = path_iter.next() {
            p
        } else {
            todo!(
                "implement producing a record when the user is asking for returning the whole thing"
            );
        };

        if path == "specrevision" {
            stack.push_literal(&mapping.spec_revision);
            return;
        }

        if path == "id" {
            stack.push_literal(&mapping.id);
            return;
        }

        if path == "time" {
            stack.push_literal(&mapping.time);
            return;
        }

        if path == "source" {
            stack.push_literal(&mapping.source);
            return;
        }

        if path == "subject" {
            stack.push_literal(&mapping.subject);
            return;
        }

        if path == "type" {
            stack.push_literal(&mapping.r#type);
            return;
        }

        if path == "datacontenttype" {
            stack.push_literal(&mapping.data_content_type);
            return;
        }

        if path == "predecessorhash" {
            stack.push_literal(&mapping.predecessorhash);
            return;
        }

        if path == "hash" {
            stack.push_literal(&mapping.hash);
            return;
        }

        if path == "data" {
            if let Some(mut payload) = mapping.data.as_ref() {
                let mut cache_key = String::new();

                while let Some(seg) = path_iter.next() {
                    if let serde_json::Value::Object(obj) = &payload
                        && let Some(value) = obj.get(seg)
                    {
                        payload = value;
                        cache_key.push(':');
                        cache_key.push_str(seg);
                        continue;
                    }

                    stack.push_null();
                    return;
                }
            }

            stack.push_null();
            return;
        }

        stack.push_null();
    }
}

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

pub struct Rec {
    pub fields: HashMap<String, Item>,
}

pub enum Item {
    Literal(Literal),
    Array(Vec<Item>),
    Record(Rec),
}

#[derive(Default)]
pub struct Stack {
    inner: Vec<Item>,
}

impl Stack {
    fn pop(&mut self) -> Option<Item> {
        self.inner.pop()
    }

    fn pop_or_bail(&mut self) -> Option<Item> {
        if let Some(item) = self.pop() {
            return Some(item);
        }

        None
    }

    fn pop_as_literal_or_bail(&mut self) -> Option<Literal> {
        if let Item::Literal(lit) = self.pop_or_bail()? {
            return Some(lit);
        }

        None
    }

    fn pop_as_string_or_bail(&mut self) -> Option<String> {
        if let Literal::String(s) = self.pop_as_literal_or_bail()? {
            return Some(s);
        }

        None
    }

    fn pop_as_number_or_bail(&mut self) -> Option<Either<i64, f64>> {
        match self.pop_as_literal_or_bail()? {
            Literal::Integral(i) => Some(Either::Left(i)),
            Literal::Float(f) => Some(Either::Right(f)),
            _ => None,
        }
    }

    fn pop_as_bool_or_bail(&mut self) -> Option<bool> {
        if let Literal::Bool(b) = self.pop_as_literal_or_bail()? {
            return Some(b);
        }

        None
    }

    fn pop_as_array_or_bail(&mut self) -> Option<Vec<Item>> {
        if let Item::Array(xs) = self.pop_or_bail()? {
            return Some(xs);
        }

        None
    }

    fn push_literal(&mut self, lit: impl IntoLiteral) {
        self.inner.push(Item::Literal(lit.into_literal()));
    }

    fn push_array(&mut self, array: Vec<Item>) {
        self.inner.push(Item::Array(array));
    }

    fn push_record(&mut self, rec: Rec) {
        self.inner.push(Item::Record(rec));
    }

    fn push_null(&mut self) {
        self.inner.push(Item::Literal(Literal::Null));
    }
}

pub fn eval_where_clause(dict: &mut Dictionary, scope: u64, instrs: Vec<Instr>) -> bool {
    eval_where_clause_opt(dict, scope, instrs).unwrap_or_default()
}

fn eval_where_clause_opt(dict: &mut Dictionary, scope: u64, instrs: Vec<Instr>) -> Option<bool> {
    let mut stack = Stack::default();

    for instr in instrs {
        match instr {
            Instr::Push(lit) => stack.push_literal(lit),

            Instr::LoadVar(var) => {
                dict.lookup(scope, &var, &mut stack);
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
                                Item::Literal(Literal::Integral(value)),
                                Item::Literal(Literal::Integral(elem)),
                            ) if *value == elem => {
                                stack.push_literal(Literal::Bool(true));
                                break;
                            }

                            (
                                Item::Literal(Literal::Float(value)),
                                Item::Literal(Literal::Float(elem)),
                            ) if *value == elem => {
                                stack.push_literal(Literal::Bool(true));
                                break;
                            }

                            (
                                Item::Literal(Literal::Bool(value)),
                                Item::Literal(Literal::Bool(elem)),
                            ) if *value == elem => {
                                stack.push_literal(Literal::Bool(true));
                                break;
                            }

                            (
                                Item::Literal(Literal::String(value)),
                                Item::Literal(Literal::String(elem)),
                            ) if value == &elem => {
                                stack.push_literal(Literal::Bool(true));
                                break;
                            }

                            (
                                Item::Literal(Literal::Subject(value)),
                                Item::Literal(Literal::Subject(elem)),
                            ) if value == &elem => {
                                stack.push_literal(Literal::Bool(true));
                                break;
                            }

                            _ => return None,
                        }
                    }

                    stack.push_literal(Literal::Bool(false));
                }

                Operation::Equal => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Item::Literal(Literal::Integral(lhs)),
                            Item::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        (
                            Item::Literal(Literal::Float(lhs)),
                            Item::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        (
                            Item::Literal(Literal::String(lhs)),
                            Item::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        (Item::Literal(Literal::Bool(lhs)), Item::Literal(Literal::Bool(rhs))) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        (
                            Item::Literal(Literal::Subject(lhs)),
                            Item::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs == rhs));
                        }

                        _ => return None,
                    }
                }

                Operation::NotEqual => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Item::Literal(Literal::Integral(lhs)),
                            Item::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        (
                            Item::Literal(Literal::Float(lhs)),
                            Item::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        (
                            Item::Literal(Literal::String(lhs)),
                            Item::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        (Item::Literal(Literal::Bool(lhs)), Item::Literal(Literal::Bool(rhs))) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        (
                            Item::Literal(Literal::Subject(lhs)),
                            Item::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs != rhs));
                        }

                        _ => return None,
                    }
                }

                Operation::LessThan => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Item::Literal(Literal::Integral(lhs)),
                            Item::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs < rhs));
                        }

                        (
                            Item::Literal(Literal::Float(lhs)),
                            Item::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs < rhs));
                        }

                        (
                            Item::Literal(Literal::String(lhs)),
                            Item::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs < rhs));
                        }

                        (Item::Literal(Literal::Bool(lhs)), Item::Literal(Literal::Bool(rhs))) => {
                            stack.push_literal(Literal::Bool(!lhs & rhs));
                        }

                        (
                            Item::Literal(Literal::Subject(lhs)),
                            Item::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs < rhs));
                        }

                        _ => return None,
                    }
                }

                Operation::GreaterThan => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Item::Literal(Literal::Integral(lhs)),
                            Item::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs > rhs));
                        }

                        (
                            Item::Literal(Literal::Float(lhs)),
                            Item::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs > rhs));
                        }

                        (
                            Item::Literal(Literal::String(lhs)),
                            Item::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs > rhs));
                        }

                        (Item::Literal(Literal::Bool(lhs)), Item::Literal(Literal::Bool(rhs))) => {
                            stack.push_literal(Literal::Bool(lhs & !rhs));
                        }

                        (
                            Item::Literal(Literal::Subject(lhs)),
                            Item::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs > rhs));
                        }

                        _ => return None,
                    }
                }

                Operation::LessThanOrEqual => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Item::Literal(Literal::Integral(lhs)),
                            Item::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        (
                            Item::Literal(Literal::Float(lhs)),
                            Item::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        (
                            Item::Literal(Literal::String(lhs)),
                            Item::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        (Item::Literal(Literal::Bool(lhs)), Item::Literal(Literal::Bool(rhs))) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        (
                            Item::Literal(Literal::Subject(lhs)),
                            Item::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs <= rhs));
                        }

                        _ => return None,
                    }
                }

                Operation::GreaterThanOrEqual => {
                    let rhs = stack.pop_or_bail()?;
                    let lhs = stack.pop_or_bail()?;

                    match (lhs, rhs) {
                        (
                            Item::Literal(Literal::Integral(lhs)),
                            Item::Literal(Literal::Integral(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        (
                            Item::Literal(Literal::Float(lhs)),
                            Item::Literal(Literal::Float(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        (
                            Item::Literal(Literal::String(lhs)),
                            Item::Literal(Literal::String(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        (Item::Literal(Literal::Bool(lhs)), Item::Literal(Literal::Bool(rhs))) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        (
                            Item::Literal(Literal::Subject(lhs)),
                            Item::Literal(Literal::Subject(rhs)),
                        ) => {
                            stack.push_literal(Literal::Bool(lhs >= rhs));
                        }

                        _ => return None,
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
                    let key = stack.pop_as_string_or_bail()?;
                    let value = stack.pop_or_bail()?;

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

                _ => return None,
            },
        }
    }

    stack.pop_as_bool_or_bail()
}
