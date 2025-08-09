use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem,
};

use crate::{
    Expr, FromSource, Literal, Pos, Query, Sort, Value, Where,
    error::RenameError,
    parser::{Source, SourceType, parse_subject},
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

pub fn rename(query: &mut Query) -> crate::Result<Scopes> {
    let mut analysis = Analysis::default();

    rename_query(&mut analysis, query)?;

    Ok(Scopes {
        inner: analysis.scopes,
    })
}

fn rename_query(analysis: &mut Analysis, query: &mut Query) -> crate::Result<()> {
    let scope = analysis.current_scope_id();

    for from_stmt in query.from_stmts.iter_mut() {
        rename_from(analysis, from_stmt)?;
    }

    analysis.set_scope(scope);
    let scope = analysis.scope_mut();

    if let Some(predicate) = query.predicate.as_mut() {
        rename_where(scope, predicate)?;
    }

    if let Some(expr) = query.group_by.as_mut() {
        rename_expr(scope, expr)?;
    }

    if let Some(sort) = query.order_by.as_mut() {
        rename_sort(scope, sort)?;
    }

    rename_expr(scope, &mut query.projection)?;

    Ok(())
}

fn rename_from(analysis: &mut Analysis, from: &mut FromSource) -> crate::Result<()> {
    let scope = {
        let scope = analysis.scope_mut();

        if scope.contains_variable(&from.ident) {
            bail!(
                from.attrs.pos,
                RenameError::VariableAlreadyExists(mem::take(&mut from.ident))
            );
        }

        scope.new_var(from.ident.clone());
        scope.id
    };

    rename_source(analysis, &mut from.source)?;
    from.attrs.scope = scope;

    Ok(())
}

fn rename_source(analysis: &mut Analysis, source: &mut Source) -> crate::Result<()> {
    rename_source_type(analysis, &mut source.inner)?;
    source.attrs.scope = analysis.current_scope_id();

    Ok(())
}

fn rename_source_type(analysis: &mut Analysis, source_type: &mut SourceType) -> crate::Result<()> {
    if let SourceType::Subquery(query) = source_type {
        analysis.new_scope();
        rename_query(analysis, query)?;
    }

    Ok(())
}

fn rename_where(scope: &mut Scope, predicate: &mut Where) -> crate::Result<()> {
    predicate.attrs.scope = scope.id;

    rename_expr(scope, &mut predicate.expr)?;

    Ok(())
}

fn rename_sort(scope: &mut Scope, sort: &mut Sort) -> crate::Result<()> {
    rename_expr(scope, &mut sort.expr)
}

fn rename_expr(scope: &mut Scope, expr: &mut Expr) -> crate::Result<()> {
    expr.attrs.scope = scope.id;

    match &mut expr.value {
        Value::Literal(_) => {}

        Value::Var(var) => {
            let mut prev = "";

            if !scope.contains_variable(&var.name) {
                bail!(
                    expr.attrs.pos,
                    RenameError::VariableDoesNotExist(mem::take(&mut var.name))
                );
            }

            for (depth, ident) in var.path.iter().enumerate() {
                if depth == 1 {
                    if prev != "data" {
                        bail!(expr.attrs.pos, RenameError::OnlyDataFieldDynAccessField);
                    }

                    scope.var_properties_mut(&var.name).add(ident.clone());
                }

                prev = ident.as_str();
            }
        }

        Value::Record(record) => {
            for expr in record.fields.values_mut() {
                rename_expr(scope, expr)?;
            }
        }

        Value::Array(exprs) => {
            for expr in exprs.iter_mut() {
                rename_expr(scope, expr)?;
            }
        }

        Value::App { params, .. } => {
            for param in params.iter_mut() {
                rename_expr(scope, param)?;
            }
        }

        Value::Binary { lhs, rhs, .. } => {
            rename_expr(scope, lhs.as_mut())?;
            rename_expr(scope, rhs.as_mut())?;
            let lhs_pos = lhs.attrs.pos;
            let rhs_pos = rhs.attrs.pos;

            // if we have a situation where the subject property is compared to a string literal, we assume it's a subject object.
            // we do replace that string literal to a subject on behalf of the user.
            match (
                lhs.as_mut().as_mut(),
                lhs_pos,
                rhs.as_mut().as_mut(),
                rhs_pos,
            ) {
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
        }

        Value::Unary { expr, .. } => {
            rename_expr(scope, expr.as_mut())?;
        }
    };

    Ok(())
}
