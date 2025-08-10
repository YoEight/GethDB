use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem,
    ptr::NonNull,
};

use crate::{
    Expr, Literal, Query, Value, Var,
    error::RenameError,
    parser::{Attributes, ExprVisitor, QueryVisitor, parse_subject},
};

pub fn rename(query: &mut Query) -> crate::Result<Scopes> {
    let mut analysis = Analysis::default();

    query.dfs_post_order(&mut analysis)?;

    Ok(Scopes {
        inner: analysis.scopes,
    })
}

struct Analysis {
    scope_id_gen: u64,
    scope: NonNull<Scope>,
    scopes: BTreeMap<u64, Scope>,
    history: Vec<u64>,
}

impl Default for Analysis {
    fn default() -> Self {
        Self {
            scope_id_gen: 0,
            scope: NonNull::dangling(),
            scopes: Default::default(),
            history: vec![],
        }
    }
}

impl Analysis {
    fn scope_id(&self) -> u64 {
        unsafe { self.scope.as_ref().id }
    }

    fn enter_scope(&mut self) {
        if self.scope_id_gen > 0 {
            self.history.push(self.scope_id());
        }

        let new_id = self.scope_id_gen;
        let new_scope = Scope::new(new_id);
        self.scope_id_gen += 1;
        self.scopes.insert(new_id, new_scope);
        self.scope = NonNull::from(self.scopes.get_mut(&new_id).unwrap());
    }

    fn exit_scope(&mut self) {
        if let Some(scope_id) = self.history.pop() {
            self.scope = NonNull::from(self.scopes.get_mut(&scope_id).unwrap());
        }
    }

    fn scope_mut(&mut self) -> &mut Scope {
        unsafe { self.scope.as_mut() }
    }
}

impl QueryVisitor for Analysis {
    type Inner<'a> = RenameExpr<'a>;

    fn enter_query(&mut self) -> crate::Result<()> {
        self.enter_scope();

        Ok(())
    }

    fn exit_query(&mut self) -> crate::Result<()> {
        self.exit_scope();

        Ok(())
    }

    fn enter_from(&mut self, attrs: &mut Attributes, ident: &str) -> crate::Result<()> {
        let scope = self.scope_mut();

        if scope.contains_variable(ident) {
            bail!(
                attrs.pos,
                RenameError::VariableAlreadyExists(ident.to_string())
            );
        }

        scope.new_var(ident.to_string());

        Ok(())
    }

    fn exit_source(&mut self, attrs: &mut Attributes) -> crate::Result<()> {
        attrs.scope = self.scope_id();

        Ok(())
    }

    fn exit_from(&mut self, attrs: &mut Attributes, _ident: &str) -> crate::Result<()> {
        attrs.scope = self.scope_id();
        Ok(())
    }

    fn exit_where_clause(&mut self, attrs: &mut Attributes, expr: &mut Expr) -> crate::Result<()> {
        attrs.scope = expr.attrs.scope;
        Ok(())
    }

    fn expr_visitor(&mut self) -> Self::Inner<'_> {
        RenameExpr { inner: self }
    }
}

struct RenameExpr<'a> {
    inner: &'a mut Analysis,
}

impl ExprVisitor for RenameExpr<'_> {
    fn on_var(&mut self, attrs: &mut Attributes, var: &mut Var) -> crate::Result<()> {
        let scope = self.inner.scope_mut();
        let mut prev = "";

        if !scope.contains_variable(&var.name) {
            bail!(
                attrs.pos,
                RenameError::VariableDoesNotExist(mem::take(&mut var.name))
            );
        }

        for (depth, ident) in var.path.iter().enumerate() {
            if depth == 1 {
                if prev != "data" {
                    bail!(attrs.pos, RenameError::OnlyDataFieldDynAccessField);
                }

                scope.var_properties_mut(&var.name).add(ident.clone());
            }

            prev = ident.as_str();
        }

        Ok(())
    }

    fn on_binary(
        &mut self,
        _attrs: &mut Attributes,
        _op: &crate::Operation,
        lhs: &mut Expr,
        rhs: &mut Expr,
    ) -> crate::Result<()> {
        let lhs_pos = lhs.attrs.pos;
        let rhs_pos = rhs.attrs.pos;

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
        Ok(())
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
    fn new(id: u64) -> Self {
        Self {
            id,
            properties: HashMap::default(),
        }
    }

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
