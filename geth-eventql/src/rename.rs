use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    Expr, From, Pos, Query, Value, Where,
    parser::{Source, SourceType},
};

pub struct Lexical {
    pos: Pos,
    scope: u64,
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
}

pub struct Scope {
    id: u64,
    properties: HashMap<String, Properties>,
}

impl Scope {
    fn contains_variable(&self, name: &str) -> bool {
        self.properties.contains_key(name)
    }

    fn new_var(&mut self, name: String) {
        self.properties.insert(name, Properties::default());
    }

    fn var_properties_mut(&mut self, name: &str) -> eyre::Result<&mut Properties> {
        if let Some(prop) = self.properties.get_mut(name) {
            return Ok(prop);
        }

        eyre::bail!("UNREACHABLE CODE PATH ERROR: variable '{name}' doesn't exist")
    }
}

pub fn rename(query: Query<Pos>) -> eyre::Result<Query<Lexical>> {
    let mut analysis = Analysis::default();

    rename_query(&mut analysis, query)
}

fn rename_query(analysis: &mut Analysis, query: Query<Pos>) -> eyre::Result<Query<Lexical>> {
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

    Ok(Query {
        tag: Lexical {
            pos: query.tag,
            scope: scope.id,
        },

        from_stmts,
        predicate,
        group_by,
        order_by: todo!(),
        limit: todo!(),
        projection: todo!(),
    })
}

fn rename_from(analysis: &mut Analysis, from: From<Pos>) -> eyre::Result<From<Lexical>> {
    let scope = {
        let scope = analysis.scope_mut();

        if scope.contains_variable(&from.ident) {
            eyre::bail!(
                "{}: variable '{}' already exists in this scope",
                from.tag,
                from.ident
            );
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

fn rename_source(analysis: &mut Analysis, source: Source<Pos>) -> eyre::Result<Source<Lexical>> {
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
) -> eyre::Result<SourceType<Lexical>> {
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

fn rename_where(scope: &mut Scope, predicate: Where<Pos>) -> eyre::Result<Where<Lexical>> {
    Ok(Where {
        tag: Lexical {
            pos: predicate.tag,
            scope: scope.id,
        },

        expr: rename_expr(scope, predicate.expr)?,
    })
}

fn rename_expr(scope: &mut Scope, expr: Expr<Pos>) -> eyre::Result<Expr<Lexical>> {
    let lexical = Lexical {
        pos: expr.tag,
        scope: scope.id,
    };

    let value = match expr.value {
        Value::Literal(l) => Value::Literal(l),

        Value::Path(path) => {
            let mut prev = "";
            let mut var_name = "";

            for (depth, ident) in path.iter().enumerate() {
                match depth {
                    0 => {
                        if !scope.contains_variable(&ident) {
                            eyre::bail!("{}: variable '{ident}' doesn't exist", expr.tag);
                        }

                        var_name = ident.as_str();
                    }

                    2 => {
                        if prev != "data" {
                            eyre::bail!(
                                "{}: only the 'data' field can have dynamically access fields",
                                expr.tag
                            );
                        }

                        scope.var_properties_mut(var_name)?.add(ident.clone());
                    }

                    _ => {}
                }

                prev = ident.as_str();
            }

            Value::Path(path)
        }

        Value::Record(record) => todo!(),
        Value::Array(exprs) => todo!(),
        Value::App { fun, params } => todo!(),
        Value::Binary { lhs, op, rhs } => todo!(),
        Value::Unary { op, expr } => todo!(),
    };

    Ok(Expr {
        tag: lexical,
        value,
    })
}
