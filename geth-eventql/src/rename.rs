use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    Expr, From, Literal, Pos, Query, Sort, Value, Where,
    error::RenameError,
    parser::{Record, Source, SourceType, parse_subject},
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Lexical {
    pub pos: Pos,
    pub scope: u64,
}

#[derive(Default)]
struct Analysis {
    scope_id_gen: u64,
    current_scope: u64,
    scopes: BTreeMap<u64, Scope>,
}

impl Analysis {
    fn scope_mut(&mut self) -> &mut Scope {
        self.scopes
            .entry(self.scope_id_gen)
            .or_insert_with(|| Scope {
                id: self.scope_id_gen,
                properties: Default::default(),
            })
    }

    fn current_scope_id(&self) -> u64 {
        self.current_scope
    }

    fn new_scope(&mut self) {
        self.scope_id_gen += 1;
        self.current_scope = self.scope_id_gen;
    }

    fn set_scope(&mut self, scope: u64) {
        self.current_scope = scope;
    }
}

#[derive(Default)]
pub struct Properties {
    pub inner: HashSet<String>,
}

impl Properties {
    fn add(&mut self, prop: String) {
        self.inner.insert(prop);
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.inner.iter()
    }
}

pub struct Scope {
    id: u64,
    properties: HashMap<String, Properties>,
}

impl Scope {
    pub fn id(&self) -> u64 {
        self.id
    }

    fn contains_variable(&self, name: &str) -> bool {
        self.properties.contains_key(name)
    }

    fn new_var(&mut self, name: String) {
        self.properties.insert(name, Properties::default());
    }

    fn var_properties_mut(&mut self, name: &str) -> &mut Properties {
        if let Some(prop) = self.properties.get_mut(name) {
            return prop;
        }

        panic!("UNREACHABLE CODE PATH ERROR: variable '{name}' doesn't exist")
    }

    pub fn var_properties(&self, name: &str) -> &Properties {
        if let Some(prop) = self.properties.get(name) {
            return prop;
        }

        panic!("UNREACHABLE CODE PATH ERROR: variable '{name}' doesn't exist")
    }

    pub fn vars(&self) -> impl Iterator<Item = (&String, &Properties)> {
        self.properties.iter()
    }
}

pub struct Scopes {
    inner: BTreeMap<u64, Scope>,
}

impl Scopes {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn scope(&self, id: u64) -> &Scope {
        if let Some(scope) = self.inner.get(&id) {
            return scope;
        }

        panic!("UNREACHABLE CODE PATH: scope '{id}' doesn't exist")
    }

    pub fn iter(&self) -> impl Iterator<Item = &Scope> {
        self.inner.values()
    }
}

pub struct Renamed {
    pub scopes: Scopes,
    pub query: Query<Lexical>,
}

pub fn rename(query: Query<Pos>) -> crate::Result<Renamed> {
    let mut analysis = Analysis::default();

    let query = rename_query(&mut analysis, query)?;

    Ok(Renamed {
        scopes: Scopes {
            inner: analysis.scopes,
        },
        query,
    })
}

fn rename_query(analysis: &mut Analysis, query: Query<Pos>) -> crate::Result<Query<Lexical>> {
    let mut from_stmts = Vec::new();
    let scope = analysis.current_scope_id();

    for from_stmt in query.from_stmts {
        from_stmts.push(rename_from(analysis, from_stmt)?);
    }

    analysis.set_scope(scope);
    let scope = analysis.scope_mut();

    let predicate = if let Some(predicate) = query.predicate {
        Some(rename_where(scope, predicate)?)
    } else {
        None
    };

    let group_by = if let Some(expr) = query.group_by {
        Some(rename_expr(scope, expr)?)
    } else {
        None
    };

    let order_by = if let Some(sort) = query.order_by {
        Some(rename_sort(scope, sort)?)
    } else {
        None
    };

    Ok(Query {
        tag: Lexical {
            pos: query.tag,
            scope: scope.id,
        },

        from_stmts,
        predicate,
        group_by,
        order_by,
        limit: query.limit,
        projection: rename_expr(scope, query.projection)?,
    })
}

fn rename_from(analysis: &mut Analysis, from: From<Pos>) -> crate::Result<From<Lexical>> {
    let scope = {
        let scope = analysis.scope_mut();

        if scope.contains_variable(&from.ident) {
            bail!(from.tag, RenameError::VariableAlreadyExists(from.ident));
        }

        scope.new_var(from.ident.clone());
        scope.id
    };

    Ok(From {
        tag: Lexical {
            pos: from.tag,
            scope,
        },
        ident: from.ident,
        source: rename_source(analysis, from.source)?,
    })
}

fn rename_source(analysis: &mut Analysis, source: Source<Pos>) -> crate::Result<Source<Lexical>> {
    Ok(Source {
        tag: Lexical {
            pos: source.tag,
            scope: analysis.current_scope_id(),
        },

        inner: rename_source_type(analysis, source.inner)?,
    })
}

fn rename_source_type(
    analysis: &mut Analysis,
    source_type: SourceType<Pos>,
) -> crate::Result<SourceType<Lexical>> {
    match source_type {
        SourceType::Events => Ok(SourceType::Events),
        SourceType::Subject(sub) => Ok(SourceType::Subject(sub)),

        SourceType::Subquery(query) => {
            analysis.new_scope();
            let query = rename_query(analysis, *query)?;

            Ok(SourceType::Subquery(Box::new(query)))
        }
    }
}

fn rename_where(scope: &mut Scope, predicate: Where<Pos>) -> crate::Result<Where<Lexical>> {
    Ok(Where {
        tag: Lexical {
            pos: predicate.tag,
            scope: scope.id,
        },

        expr: rename_expr(scope, predicate.expr)?,
    })
}

fn rename_sort(scope: &mut Scope, sort: Sort<Pos>) -> crate::Result<Sort<Lexical>> {
    Ok(Sort {
        expr: rename_expr(scope, sort.expr)?,
        order: sort.order,
    })
}

fn rename_expr(scope: &mut Scope, expr: Expr<Pos>) -> crate::Result<Expr<Lexical>> {
    let lexical = Lexical {
        pos: expr.tag,
        scope: scope.id,
    };

    let value = match expr.value {
        Value::Literal(l) => Value::Literal(l),

        Value::Var(var) => {
            let mut prev = "";

            if !scope.contains_variable(&var.name) {
                bail!(expr.tag, RenameError::VariableDoesNotExist(var.name));
            }

            for (depth, ident) in var.path.iter().enumerate() {
                if depth == 1 {
                    if prev != "data" {
                        bail!(expr.tag, RenameError::OnlyDataFieldDynAccessField);
                    }

                    scope.var_properties_mut(&var.name).add(ident.clone());
                }

                prev = ident.as_str();
            }

            Value::Var(var)
        }

        Value::Record(record) => {
            let mut fields = HashMap::new();

            for (field, expr) in record.fields {
                fields.insert(field, rename_expr(scope, expr)?);
            }

            Value::Record(Record { fields })
        }

        Value::Array(exprs) => {
            let mut values = Vec::new();

            for expr in exprs {
                values.push(rename_expr(scope, expr)?);
            }

            Value::Array(values)
        }

        Value::App { fun, params } => {
            let mut new_params = Vec::new();

            for param in params {
                new_params.push(rename_expr(scope, param)?);
            }

            Value::App {
                fun,
                params: new_params,
            }
        }

        Value::Binary { lhs, op, rhs } => {
            let mut lhs = rename_expr(scope, *lhs)?;
            let mut rhs = rename_expr(scope, *rhs)?;
            let lhs_pos = lhs.tag.pos;
            let rhs_pos = rhs.tag.pos;

            // if we have a situation where the subject property is compared to a string literal, we assume it's a subject object.
            // we do replace that string literal to a subject on behalf of the user.
            match (lhs.as_mut(), lhs_pos, rhs.as_mut(), rhs_pos) {
                (Value::Var(v), _, Value::Literal(lit), pos)
                | (Value::Literal(lit), pos, Value::Var(v), _)
                    if lit.is_string() =>
                {
                    if v.path.as_slice() == ["subject"] {
                        let sub = parse_subject(pos, lit.as_str().expect("to be defined"))?;
                        *lit = Literal::Subject(sub);
                    }
                }

                _ => {}
            }

            Value::Binary {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            }
        }

        Value::Unary { op, expr } => Value::Unary {
            op,
            expr: Box::new(rename_expr(scope, *expr)?),
        },
    };

    Ok(Expr {
        tag: lexical,
        value,
    })
}
